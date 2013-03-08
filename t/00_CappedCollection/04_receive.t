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
my $port = Net::EmptyPort::empty_port( 32637 ); # 32637-32766 Unassigned

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
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );
$skip_msg = "Need a Redis server version 2.6 or higher" if ( !$skip_msg and !eval { return $real_redis->eval( 'return 1', 0 ) } );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For real Redis:
#$redis = $real_redis;
#isa_ok( $redis, 'Redis' );

# For Test::RedisServer
$real_redis->quit;
$redis = Test::RedisServer->new( conf => { port => $port }, timeout => 3 ) unless $redis;
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

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

# some inserts
for ( my $i = 1; $i <= 10; ++$i )
{
    $coll->insert( $_, $i ) for $i..10;
}

#-- all correct

for ( my $i = 1; $i <= 10; ++$i )
{
    @arr = ();
    push @arr, $_ for $i..10;
    @arr = sort @arr;
    my @ret = sort $coll->receive( $i );
    is "@arr", "@ret", "correct receive";
}

@arr = ( 1, 2, 3 );
@arr = $coll->receive( "bad_id" );
ok !@arr, "not received";

for ( my $i = 1; $i <= 10; ++$i )
{
    $tmp = $coll->receive( 1, $i - 1 );
    is $tmp, $i.'', "correct receive";
}
$tmp = $coll->receive( 1, 123 );
ok !defined( $tmp ), "empty list";
$tmp = $coll->receive( 1 );
is $tmp, 10, "correct list len";

$coll = Redis::CappedCollection->new(
    $redis,
    );
$coll->insert( $_, "Some id" ) for 1..10;
@arr = ();
push @arr, ( $_ - 1, $_ ) for 1..10;
my @ret = $coll->receive( "Some id", '' );
is "@arr", "@ret", "correct receive";

$tmp = $coll->receive( "Some id", 'bad_id' );
is $tmp, undef, "correct receive";

# errors in the arguments

dies_ok { $coll->receive() } "expecting to die - no args";

foreach my $arg ( ( undef, "", \"scalar", [], $uuid ) )
{
    dies_ok { $coll->receive( $arg ) } "expecting to die: ".( $arg || '' );
}

foreach my $arg ( ( \"scalar", [], $uuid ) )
{
    dies_ok { $coll->receive( 1, $arg ) } "expecting to die: ".( $arg || '' );
}

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
