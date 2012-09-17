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
    EDATAIDEXISTS
    EOLDERTHANALLOWED
    );

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $redis;
my $real_redis;
my $port = empty_port();

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
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
$redis = Test::RedisServer->new( conf => { port => $port }, timeout => 3 );
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $status_key, $queue_key, $size, $size_garbage, $maxmemory, @arr );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $redis = Test::RedisServer->new( conf =>
        {
            port                => empty_port(),
            maxmemory           => $maxmemory,
#            "vm-enabled"        => 'no',
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->new(
        $redis,
        $size         ? ( 'size'         => $size         ) : (),
        $size_garbage ? ( 'size_garbage' => $size_garbage ) : (),
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$size_garbage = $size = 0;
$maxmemory = 0;
new_connect();
is $coll->size_garbage, 0, $msg;
$coll->drop;

$size_garbage = 12345;
dies_ok { new_connect() } "expecting to die: 12345 > $size";

$size = 100_000;
$size_garbage = 50_000;
new_connect();
is $coll->size_garbage, $size_garbage, $msg;

$coll->insert( '*' x 10_000 ) for 1..10;
@arr = $coll->validate;
is $arr[0], 100_000, "correct value";
$name = 'TEST';
( $name, $tmp ) = $coll->insert( '*', $name );
@arr = $coll->validate;
is $arr[0], 40_001, "correct value";
$coll->insert( '*' x 10_000, $name );
@arr = $coll->validate;
is $arr[0], 50_001, "correct value";

$coll->update( $name, $tmp, '*' x 10_000 );
@arr = $coll->validate;
is $arr[0], 60_000, "correct value";

$coll->insert( '*' x 10_000, $name ) for 1..4;
@arr = $coll->validate;
is $arr[0], 100_000, "correct value";
$coll->drop;

$size = 10;
$size_garbage = 0;
new_connect();
$tmp = 'A';
$coll->insert( $tmp++, $name ) for 1..10;
@arr = $coll->receive( $name );
is "@arr", "A B C D E F G H I J", "correct value";
$coll->insert( $tmp++, $name );
@arr = $coll->receive( $name );
is "@arr", "B C D E F G H I J K", "correct value";
$coll->insert( $tmp++ x 2, $name );
@arr = $coll->receive( $name );
is "@arr", "D E F G H I J K LL", "correct value";

$size = 10;
$size_garbage = 5;
new_connect();
$tmp = 'A';
$coll->insert( $tmp++, $name ) for 1..10;
@arr = $coll->receive( $name );
# data_id = 0 1 2 3 4 5 6 7 8 9
is "@arr", "A B C D E F G H I J", "correct value";
$coll->insert( $tmp++, $name );
@arr = $coll->receive( $name );
# data_id = 6 7 8 9 10
is "@arr", "G H I J K", "correct value";
$coll->update( $name, 6, $tmp++ );
@arr = $coll->receive( $name );
# data_id = 6 7 8 9 10
is "@arr", "L H I J K", "correct value";
$coll->insert( $tmp++, $name ) for 1..5;
@arr = $coll->receive( $name );
# data_id = 6 7 8 9 10
is "@arr", "L H I J K M N O P Q", "correct value";
$coll->update( $name, 6, '**' );
@arr = $coll->receive( $name );
is "@arr", "N O P Q", "correct value";

foreach my $arg ( ( undef, 0.5, -1, -3, "", "0.5", \"scalar", [], $uuid ) )
{
    dies_ok { $coll = Redis::CappedCollection->new(
        redis           => DEFAULT_SERVER.":".empty_port(),
        size_garbage    => $arg,
        ) } "expecting to die: ".( $arg || '' );
}

}
