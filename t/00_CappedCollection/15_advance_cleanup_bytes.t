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
use Time::HiRes     qw( gettimeofday );
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

my ( $coll, $name, $tmp, $status_key, $queue_key, $size, $advance_cleanup_bytes, $maxmemory, @arr );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $redis->stop if $redis;
    $redis = get_redis( $redis, conf =>
        {
            port                => Net::EmptyPort::empty_port( DEFAULT_PORT ),
            maxmemory           => $maxmemory,
#            "vm-enabled"        => 'no',
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    skip( $redis_error, 1 ) unless $redis;
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->create(
        $redis,
        $size          ? ( 'size'         => $size         ) : (),
        $advance_cleanup_bytes ? ( 'advance_cleanup_bytes' => $advance_cleanup_bytes ) : (),
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$advance_cleanup_bytes = $size = 0;
$maxmemory = 0;
new_connect();
is $coll->advance_cleanup_bytes, 0, $msg;
$coll->drop_collection;

$advance_cleanup_bytes = 12345;
$size = 123;
dies_ok { new_connect() } "expecting to die: 12345 > $size";

$size = 100_000;
$advance_cleanup_bytes = 50_000;
new_connect();
is $coll->advance_cleanup_bytes, $advance_cleanup_bytes, $msg;

$coll->insert( '*' x 10_000 ) for 1..10;
is $coll->collection_info->{length}, 100_000, "correct value";
$name = 'TEST';
( $name, $tmp ) = $coll->insert( '*', $name );
is $coll->collection_info->{length}, 40_001, "correct value";
$coll->insert( '*' x 10_000, $name );
is $coll->collection_info->{length}, 50_001, "correct value";

$coll->update( $name, $tmp, '*' x 10_000 );
is $coll->collection_info->{length}, 60_000, "correct value";

$coll->insert( '*' x 10_000, $name ) for 1..4;
is $coll->collection_info->{length}, 100_000, "correct value";

dies_ok { $coll->advance_cleanup_bytes( -1 ) } "expecting to die: advance_cleanup_bytes is negative";
dies_ok { $coll->advance_cleanup_bytes( $coll->size + 1 ) } "expecting to die: advance_cleanup_bytes > size";

$coll->drop_collection;

$size = 10;
$advance_cleanup_bytes = 0;
new_connect();
$tmp = 'A';
$coll->insert( $tmp++, $name, undef, gettimeofday + 0 ) for 1..10;
@arr = $coll->receive( $name );
is "@arr", "A B C D E F G H I J", "correct value";
$coll->insert( $tmp++, $name, undef, gettimeofday + 0 );
@arr = $coll->receive( $name );
is "@arr", "B C D E F G H I J K", "correct value";
$coll->insert( $tmp++ x 2, $name, undef, gettimeofday + 0 );
@arr = $coll->receive( $name );
is "@arr", "D E F G H I J K LL", "correct value";

$size = 10;
$advance_cleanup_bytes = 5;
new_connect();
$tmp = 'A';
$coll->insert( $tmp++, $name, undef, gettimeofday + 0 ) for 1..10;
@arr = $coll->receive( $name, undef, gettimeofday + 0 );
@arr = sort @arr;
# data_id = 0 1 2 3 4 5 6 7 8 9
is "@arr", "A B C D E F G H I J", "correct value";
$coll->insert( $tmp++, $name, undef, gettimeofday + 0 );
@arr = $coll->receive( $name );
@arr = sort @arr;
# data_id = 6 7 8 9 10
#           G H I J K
is "@arr", "G H I J K", "correct value";
$coll->update( $name, 6, $tmp++ );
@arr = $coll->receive( $name );
@arr = sort @arr;
# data_id = 6 7 8 9 10
#           L H I J K
is "@arr", "H I J K L", "correct value";
$coll->insert( $tmp++, $name, undef, gettimeofday + 0 ) for 1..5;
@arr = $coll->receive( $name );
@arr = sort @arr;
# data_id = 6 7 8 9 10
#           L H I J K M N O P Q
is "@arr", "H I J K L M N O P Q", "correct value";
@arr = sort @arr;
$coll->update( $name, 6, '**' );
@arr = $coll->receive( $name );
is "@arr", "N O P Q", "correct value";

foreach my $arg ( ( undef, 0.5, -1, -3, "", "0.5", \"scalar", [], $uuid ) )
{
    dies_ok { $coll = Redis::CappedCollection->create(
        redis                   => DEFAULT_SERVER.":".Net::EmptyPort::empty_port( DEFAULT_PORT ),
        advance_cleanup_bytes   => $arg,
        ) } "expecting to die: ".( $arg || '' );
}

}
