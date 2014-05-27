#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib', 't/tlib';

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

use Redis::CappedCollection::Test::Utils qw(
    get_redis
    verify_redis
);

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my $redis_error = "Unable to create test Redis server";
my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $status_key, $queue_key, $size, $advance_cleanup_bytes, $advance_cleanup_num, $maxmemory, @arr, $info );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $redis->stop if $redis;
    $redis = get_redis( $redis, conf =>
        {
            port                => Net::EmptyPort::empty_port( DEFAULT_PORT ),
            maxmemory           => $maxmemory,
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    skip( $redis_error, 1 ) unless $redis;
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->new(
        $redis,
        $size ? ( 'size' => $size ) : (),
        $advance_cleanup_bytes ? ( 'advance_cleanup_bytes' => $advance_cleanup_bytes ) : (),
        $advance_cleanup_num   ? ( 'advance_cleanup_num'   => $advance_cleanup_num   ) : (),
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$advance_cleanup_num = $size = 0;
$maxmemory = 0;
new_connect();
is $coll->advance_cleanup_num, 0, $msg;
$coll->drop_collection;

$size = 100_000;
$advance_cleanup_num = 5;
new_connect();
is $coll->advance_cleanup_num, $advance_cleanup_num, $msg;
$coll->drop_collection;

$advance_cleanup_bytes = $size = 0;
$maxmemory = 0;
new_connect();
is $coll->advance_cleanup_bytes, 0, $msg;
$coll->drop_collection;

$size = 100_000;
$advance_cleanup_bytes = 50_000;
new_connect();
is $coll->advance_cleanup_bytes, $advance_cleanup_bytes, $msg;

$coll->insert( '*' x 10_000 ) for 1..10;
is $coll->collection_info->{length}, 100_000, "correct value";

$coll->advance_cleanup_num( 3 );

$name = 'TEST';
( $name, $tmp ) = $coll->insert( '*', $name );
$info = $coll->collection_info;
is $info->{length}, 70_001, "correct value";
is $info->{items}, 8, "correct value";

$coll->insert( '*' x 10_000, $name );
$info = $coll->collection_info;
is $info->{length}, 80_001, "correct value";
is $info->{items}, 9, "correct value";

$coll->update( $name, $tmp, '*' x 10_000 );
$info = $coll->collection_info;
is $info->{length}, 90_000, "correct value";

$coll->insert( '*' x 10_000, $name );
$info = $coll->collection_info;
is $info->{length}, 100_000, "correct value";

$coll->advance_cleanup_num( 6 );

$name = 'TEST';
( $name, $tmp ) = $coll->insert( '*', $name );
$info = $coll->collection_info;
is $info->{length}, 40_001, "correct value";
is $info->{items}, 5, "correct value";

$coll->drop_collection;

$size = 10;
$advance_cleanup_bytes = 5;
$advance_cleanup_num   = 3;
new_connect();

$tmp = 'A';
$coll->insert( $tmp++, $name ) for 1..10;
@arr = $coll->receive( $name );
is "@arr", "A B C D E F G H I J", "correct value";
$coll->insert( '**', $name );
@arr = sort $coll->receive( $name );
is "@arr", "** D E F G H I J", "correct value";

foreach my $arg ( ( undef, 0.5, -1, -3, "", "0.5", \"scalar", [], $uuid ) )
{
    dies_ok { $coll = Redis::CappedCollection->new(
        redis               => DEFAULT_SERVER.":".Net::EmptyPort::empty_port( DEFAULT_PORT ),
        advance_cleanup_num => $arg,
        ) } "expecting to die: ".( $arg || '' );
}

}
