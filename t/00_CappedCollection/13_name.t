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

my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $status_key, $queue_key );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $redis->stop if $redis;
    $redis = get_redis( conf =>
        {
            port                => Net::EmptyPort::empty_port( DEFAULT_PORT ),
            maxmemory           => 0,
#            "vm-enabled"        => 'no',
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->new(
        $redis,
        $name ? ( 'name' => $name ) : (),
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$name = '';
new_connect();
is bytes::length( $coll->name ), bytes::length( '89116152-C5BD-11E1-931B-0A690A986783' ), $msg;
$tmp = $coll->drop_collection;
is $tmp, 1, "correct";

new_connect();
$coll->insert( '*');
$tmp = $coll->drop_collection;
is $tmp, 5, "correct";

$name = $msg;
new_connect();

is $coll->name, $msg, $msg;

$coll->drop_collection;

foreach my $arg ( ( undef, "", "Some:id", \"scalar", [], $uuid ) )
{
    dies_ok { $coll = Redis::CappedCollection->new(
        redis   => DEFAULT_SERVER.":".Net::EmptyPort::empty_port( DEFAULT_PORT ),
        name    => $arg,
        ) } "expecting to die: ".( $arg || '' );
}

}
