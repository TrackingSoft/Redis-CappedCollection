#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";                 ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";               ## no critic
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Net::EmptyPort";                  ## no critic
    plan skip_all => "because Net::EmptyPort required for testing" if $@;
}

use bytes;
use Data::UUID;
use Redis::CappedCollection qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    NAMESPACE

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EMAXMEMORYPOLICY
    ECOLLDELETED
    EREDIS
    EDATAIDEXISTS
    EOLDERTHANALLOWED
    );

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my $redis;
my $real_redis;
my $port = Net::EmptyPort::empty_port( DEFAULT_PORT );

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
if ( !$real_redis )
{
    $redis = eval { Test::RedisServer->new( conf => { port => $port }, timeout => 3 ) };
    if ( $redis )
    {
        eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".$port ) };
    }
}
my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ && $real_redis && $real_redis->ping );
$skip_msg = "Need a Redis server version 2.6 or higher" if ( !$skip_msg && !eval { return $real_redis->eval( 'return 1', 0 ) } );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
$real_redis->quit;

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $maxmemory, $policy, $size, $older_allowed, $info );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $port = Net::EmptyPort::empty_port( DEFAULT_PORT );
    $redis = Test::RedisServer->new( conf =>
        {
            port                => $port,
            maxmemory           => $maxmemory,
#            "vm-enabled"        => 'no',
            "maxmemory-policy"  => $policy,
            "maxmemory-samples" => 100,
        } );
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->new(
        $redis,
        $size ? ( size  => $size ) : (),
        older_allowed   => $older_allowed,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$maxmemory = 0;
$policy = "noeviction";
$size = 0;
$older_allowed = 1;
new_connect();

#-- all correct

# some inserts
$len = 0;
$tmp = 0;
for ( my $i = 1; $i <= 10; ++$i )
{
    ( $coll->insert( $_, $i ), $tmp += bytes::length( $_.'' ), ++$len ) for $i..10;
}
$info = $coll->collection_info;
is $info->{length}, $tmp,   "OK length - $info->{length}";
is $info->{lists},  10,     "OK lists - $info->{lists}";
is $info->{items},  $len,   "OK queue length - $info->{items}";

#-- ENOERROR

is $coll->last_errorcode, ENOERROR, "ENOERROR";
note '$@: ', $@;

#-- EMISMATCHARG

eval { $coll->insert() };
is $coll->last_errorcode, EMISMATCHARG, "EMISMATCHARG";
note '$@: ', $@;

#-- EDATATOOLARGE

my $prev_max_datasize = $coll->max_datasize;
my $max_datasize = 100;
$coll->max_datasize( $max_datasize );
is $coll->max_datasize, $max_datasize, $msg;

eval { $id = $coll->insert( '*' x ( $max_datasize + 1 ) ) };
is $coll->last_errorcode, EDATATOOLARGE, "EDATATOOLARGE";
note '$@: ', $@;
$coll->max_datasize( $prev_max_datasize );

#-- ENETWORK

$coll->quit;

eval { $info = $coll->collection_info };
is $coll->last_errorcode, ENETWORK, "ENETWORK";
note '$@: ', $@;
ok !$coll->_redis->ping, "server is not available";

new_connect();

#-- EMAXMEMORYLIMIT

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

    $maxmemory = 1024 * 1024;
    new_connect();
    my ( undef, $max_datasize ) = $coll->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
    is $max_datasize, $maxmemory, "value is set correctly";

    $tmp = '*' x 1024;
    for ( my $i = 0; $i < 2 * 1024; ++$i )
    {
        eval { $id = $coll->insert( $tmp, $i.'' ) };
        if ( $@ )
        {
            is $coll->last_errorcode, EMAXMEMORYLIMIT, "EMAXMEMORYLIMIT";
            note "($i)", '$@: ', $@;
            last;
        }
    }

    $coll->drop_collection;
}

#-- EMAXMEMORYPOLICY

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

#    $policy = "volatile-lru";       # -> remove the key with an expire set using an LRU algorithm
#    $policy = "allkeys-lru";        # -> remove any key accordingly to the LRU algorithm
#    $policy = "volatile-random";    # -> remove a random key with an expire set
    $policy = "allkeys-random";     # -> remove a random key, any key
#    $policy = "volatile-ttl";       # -> remove the key with the nearest expire time (minor TTL)
#    $policy = "noeviction";         # -> don't expire at all, just return an error on write operations

    dies_ok { new_connect() } "expecting to die: EMAXMEMORYPOLICY";
}

#-- ECOLLDELETED

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

#    $policy = "volatile-lru";       # -> remove the key with an expire set using an LRU algorithm
#    $policy = "allkeys-lru";        # -> remove any key accordingly to the LRU algorithm
#    $policy = "volatile-random";    # -> remove a random key with an expire set
#    $policy = "allkeys-random";     # -> remove a random key, any key
#    $policy = "volatile-ttl";       # -> remove the key with the nearest expire time (minor TTL)
    $policy = "noeviction";         # -> don't expire at all, just return an error on write operations

    new_connect();
    $status_key  = NAMESPACE.':status:'.$coll->name;

    $id = $coll->insert( '*' x 1024 );
    $coll->_call_redis( 'DEL', $status_key );
    eval { $id = $coll->insert( '*' x 1024 ) };
    ok $@, "exception";
    is $coll->last_errorcode, ECOLLDELETED, "ECOLLDELETED";
    note '$@: ', $@;
    $coll->drop_collection;
}

#-- EREDIS

eval { $coll->_call_redis( "BADTHING", "Anything" ) };
is $coll->last_errorcode, EREDIS, "EREDIS";
note '$@: ', $@;

#-- EDATAIDEXISTS

new_connect();
$id = $coll->insert( '*' x 1024, "Some id", 123 );
eval { $id = $coll->insert( '*' x 1024, "Some id", 123 ) };
is $coll->last_errorcode, EDATAIDEXISTS, "EDATAIDEXISTS";
note '$@: ', $@;

#-- EOLDERTHANALLOWED

$size = 5;
new_connect();

$list_key = NAMESPACE.':D:*';
foreach my $i ( 1..( $coll->size * 2 ) )
{
    $id = $coll->insert( $i, "Some id", $i, $i );
}
eval { $id = $coll->insert( '*', "Some id", 123, 1 ) };
is $coll->last_errorcode, ENOERROR, "ENOERROR";
note '$@: ', $@;

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

$older_allowed = 0;
new_connect();

$list_key = NAMESPACE.':D:*';
foreach my $i ( 1..( $coll->size * 2 ) )
{
    $id = $coll->insert( $i, "Some id", $i, $i );
}
eval { $id = $coll->insert( '*', "Some id", 123, 1 ) };
is $coll->last_errorcode, EOLDERTHANALLOWED, "EOLDERTHANALLOWED";
note '$@: ', $@;

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
