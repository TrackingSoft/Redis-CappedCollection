#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Test::TCP";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
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
    );

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my $redis;
my $real_redis;
my $port;

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );
$skip_msg = "Need a Redis server version 2.6 or higher" if ( !$skip_msg and !eval { return $real_redis->eval( 'return 1', 0 ) } );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
$real_redis->quit;

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $maxmemory, $policy );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $port = empty_port();
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
new_connect();

#-- all correct

# some inserts
$len = 0;
$tmp = 0;
for ( my $i = 1; $i <= 10; ++$i )
{
    ( $coll->insert( $_, $i ), $tmp += bytes::length( $_.'' ), ++$len ) for $i..10;
}
@arr = $coll->validate;
is $arr[0], $tmp,   "OK length - $arr[0]";
is $arr[1], 10,     "OK lists - $arr[1]";
is $arr[2], $len,   "OK queue length - $arr[2]";

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

eval { @arr = $coll->validate };
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

    $coll->drop;
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

    $maxmemory = 2 * 1024 * 1024;
    {
        new_connect();
        my ( undef, $max_datasize ) = $coll->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
        is $max_datasize, $maxmemory, "value is set correctly";

        $tmp = '*' x ( 1024 * 3 );

        eval { $id = $coll->insert( $tmp, $_ ) } for ( 1..1024 );

        eval {
            while ( @arr = $coll->pop_oldest )
            {
                ;
            }
        };
        redo unless $coll->last_errorcode == EMAXMEMORYPOLICY;
        ok $@, "exception";
        is $coll->last_errorcode, EMAXMEMORYPOLICY, "EMAXMEMORYPOLICY";
        note '$@: ', $@;

        $coll->drop;
    }
}

#-- ECOLLDELETED

SKIP:
{
    skip( 'because Test::RedisServer required for that test', 1 ) if eval { $real_redis->ping };

#    $policy = "volatile-lru";       # -> remove the key with an expire set using an LRU algorithm
#    $policy = "allkeys-lru";        # -> remove any key accordingly to the LRU algorithm
#    $policy = "volatile-random";    # -> remove a random key with an expire set
    $policy = "allkeys-random";     # -> remove a random key, any key
#    $policy = "volatile-ttl";       # -> remove the key with the nearest expire time (minor TTL)
#    $policy = "noeviction";         # -> don't expire at all, just return an error on write operations

    $maxmemory = 2 * 1024 * 1024;
    {
        new_connect();
        $status_key  = NAMESPACE.':status:'.$coll->name;
        my ( undef, $max_datasize ) = $coll->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
        is $max_datasize, $maxmemory, "value is set correctly";

        $tmp = '*' x ( 1024 * 3 );

        eval { $id = $coll->insert( $tmp, $_ ) } for ( 1..1024 );

        $coll->_call_redis( 'DEL', $status_key );

        eval {
            while ( @arr = $coll->pop_oldest )
            {
                ;
            }
        };
        redo unless $coll->last_errorcode == ECOLLDELETED;
        ok $@, "exception";
        is $coll->last_errorcode, ECOLLDELETED, "ECOLLDELETED";
        note '$@: ', $@;

        $coll->drop;
    }
}

#-- EREDIS

eval { $coll->_call_redis( "BADTHING", "Anything" ) };
is $coll->last_errorcode, EREDIS, "EREDIS";
note '$@: ', $@;

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
