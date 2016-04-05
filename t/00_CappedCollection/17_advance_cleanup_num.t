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

BEGIN {
    eval 'use Test::NoWarnings';                ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

use bytes;
use Data::UUID;
use Params::Util qw(
    _NUMBER
);
use Redis::CappedCollection qw(
    $DEFAULT_SERVER
    $DEFAULT_PORT
    $NAMESPACE
    $DEFAULT_ADVANCE_CLEANUP_NUM
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

my ( $coll, $name, $tmp, $status_key, $queue_key, $advance_cleanup_bytes, $advance_cleanup_num, $maxmemory, @arr, $info );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

my $data_id = 0;

sub new_connect {
    # For Test::RedisServer
    $redis->stop if $redis;
    $port = Net::EmptyPort::empty_port( $port );
    $redis = get_redis( conf =>
        {
            port                => $port,
            maxmemory           => $maxmemory,
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    skip( $redis_error, 1 ) unless $redis;
    isa_ok( $redis, 'Test::RedisServer' );

    $data_id = 0;

    $coll = Redis::CappedCollection->create(
        redis   => $redis,
        name    => $uuid->create_str,
        'older_allowed' => 1,
        $advance_cleanup_bytes ? ( 'advance_cleanup_bytes' => $advance_cleanup_bytes ) : (),
        $advance_cleanup_num   ? ( 'advance_cleanup_num'   => $advance_cleanup_num   ) : (),
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = $NAMESPACE.':S:'.$coll->name;
    $queue_key   = $NAMESPACE.':Q:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$advance_cleanup_num = 0;
$maxmemory = 0;
new_connect();
is $coll->advance_cleanup_num, $DEFAULT_ADVANCE_CLEANUP_NUM, $msg;
$coll->drop_collection;

$advance_cleanup_num = 5;
new_connect();
is $coll->advance_cleanup_num, $advance_cleanup_num, $msg;
$coll->drop_collection;

$advance_cleanup_bytes = 0;
$maxmemory = 0;
new_connect();
is $coll->advance_cleanup_bytes, 0, $msg;
$coll->drop_collection;

$advance_cleanup_bytes = 50_000;
new_connect();
is $coll->advance_cleanup_bytes, $advance_cleanup_bytes, $msg;

$coll->insert( 'List id', $data_id++, '*' x 10_000 ) for 1..10;

$coll->resize( advance_cleanup_num => 3 );

$name = 'TEST';
$tmp = $data_id;
$coll->insert( $name, $data_id++, '*' );
$info = $coll->collection_info;
is $info->{items}, 11, "correct value";
ok defined( _NUMBER( $info->{last_removed_time} ) ) && $info->{last_removed_time} >= 0, 'last_removed_time OK';

$coll->insert( $name, $data_id++, '*' x 10_000 );
$info = $coll->collection_info;
is $info->{items}, 12, "correct value";
ok defined( _NUMBER( $info->{last_removed_time} ) ) && $info->{last_removed_time} >= 0, 'last_removed_time OK';

$coll->update( $name, $tmp, '*' x 10_000 );
$info = $coll->collection_info;
ok defined( _NUMBER( $info->{last_removed_time} ) ) && $info->{last_removed_time} >= 0, 'last_removed_time OK';

$coll->insert( $name, $data_id++, '*' x 10_000 );
$info = $coll->collection_info;
ok defined( _NUMBER( $info->{last_removed_time} ) ) && $info->{last_removed_time} >= 0, 'last_removed_time OK';

$coll->resize( advance_cleanup_num => 6 );

$coll->drop_collection;

new_connect();

foreach my $arg ( ( undef, 0.5, -1, -3, "", "0.5", \"scalar", [], $uuid ) )
{
    dies_ok { $coll = Redis::CappedCollection->create(
        redis               => $redis,
        name                => $uuid->create_str,
        advance_cleanup_num => $arg,
        ) } "expecting to die: ".( $arg || '' );
}

}
