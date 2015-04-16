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

my $redis_error = "Unable to create test Redis server";
my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $maxmemory, $info );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

my $maxmemory_mode;
sub new_connect {
    # For Test::RedisServer
    $redis->stop if $redis;
    $redis = get_redis( $redis, conf =>
        {
            port                => $port,
            maxmemory           => 0,
#            "vm-enabled"        => 'no',
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    skip( $redis_error, 1 ) unless $redis;
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->create(
        $redis,
        defined( $maxmemory_mode ) ? ( check_maxmemory => $maxmemory_mode ) : (),
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

#-- check_maxmemory argument

$maxmemory_mode = 0;
new_connect();
is $coll->_maxmemory_policy, 'volatile-lru', 'check_maxmemory correct';
$maxmemory_mode = 1;
new_connect();
is $coll->_maxmemory_policy, 'noeviction', 'check_maxmemory correct';

undef $maxmemory_mode;
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

my $prev_max_datasize = $coll->max_datasize;
my $max_datasize = 100;
$coll->max_datasize( $max_datasize );
is $coll->max_datasize, $max_datasize, $msg;

eval { $id = $coll->insert( '*' x ( $max_datasize + 1 ) ) };
is $coll->last_errorcode, EDATATOOLARGE, "EDATATOOLARGE";
note '$@: ', $@;
$info = $coll->collection_info;
is $info->{length}, $tmp,   "OK length - $info->{length}";
is $info->{lists},  10,     "OK lists - $info->{lists}";
is $info->{items},  $len,   "OK queue length - $info->{items}";

eval { $id = $coll->update( '1', 0, '*' x ( $max_datasize + 1 ) ) };
is $coll->last_errorcode, EDATATOOLARGE, "EDATATOOLARGE";
note '$@: ', $@;
$info = $coll->collection_info;
is $info->{length}, $tmp,   "OK length - $info->{length}";
is $info->{lists},  10,     "OK lists - $info->{lists}";
is $info->{items},  $len,   "OK queue length - $info->{items}";

$coll->max_datasize( $prev_max_datasize );
is $coll->max_datasize, $prev_max_datasize, $msg;

eval { $id = $coll->insert( '*' x ( $max_datasize + 1 ) ) };
ok !$@, $msg;
$info = $coll->collection_info;
is $info->{length}, $tmp += $max_datasize + 1,  "OK length - $info->{length}";
is $info->{lists},  11,                         "OK lists - $info->{lists}";
is $info->{items},  ++$len,                     "OK queue length - $info->{items}";

eval { $id = $coll->update( '1', 0, '*' x ( $max_datasize + 1 ) ) };
ok !$@, $msg;
$info = $coll->collection_info;
is $info->{length}, $tmp + $max_datasize,   "OK length - $info->{length}";
is $info->{lists},  11,                     "OK lists - $info->{lists}";
is $info->{items},  $len,                   "OK queue length - $info->{items}";

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
