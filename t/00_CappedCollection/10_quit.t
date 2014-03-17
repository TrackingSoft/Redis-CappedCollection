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

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $info, $size );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

$size = 1_000_000;
$coll = Redis::CappedCollection->new(
    $redis,
    name    => "Some name",
    size    => $size,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;

$status_key  = NAMESPACE.':status:'.$coll->name;
$queue_key   = NAMESPACE.':queue:'.$coll->name;
ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";

is $coll->name, "Some name",    "correct collection name";
is $coll->size, $size,      "correct size";

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

#-- all correct

ok $coll->_redis->ping, "server is available";
$coll->quit;
ok !$coll->_redis->ping, "no server";

$coll = Redis::CappedCollection->new(
    $redis,
    name    => "Some name",
    size    => $size,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;

$status_key  = NAMESPACE.':status:'.$coll->name;
$queue_key   = NAMESPACE.':queue:'.$coll->name;
ok $coll->_call_redis( "EXISTS", $status_key ), "status hash exists";
ok $coll->_call_redis( "EXISTS", $queue_key ), "queue list exists";

is $coll->name, "Some name",    "correct collection name";
is $coll->size, $size,      "correct size";

$info = $coll->collection_info;
is $info->{length}, $tmp,   "OK length - $info->{length}";
is $info->{lists},  10,     "OK lists - $info->{lists}";
is $info->{items},  $len,   "OK queue length - $info->{items}";

#-- ping

$coll = Redis::CappedCollection->new(
    $redis,
    name    => "Some name",
    size    => $size,
    );
isa_ok( $coll, 'Redis::CappedCollection' );

ok $coll->ping, "server is available";
$coll->quit;
ok !$coll->ping, "no server";

}
