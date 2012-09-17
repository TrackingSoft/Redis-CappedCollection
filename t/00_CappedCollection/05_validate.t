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

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

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

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len );
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

#-- all correct

@arr = $coll->validate;
is $arr[0], 0, "OK length";
is $arr[1], 0, "OK lists";
is $arr[2], 0, "OK items";

# some inserts
$len = 0;
$tmp = 0;
for ( my $i = 1; $i <= 10; ++$i )
{
    ( $coll->insert( $_, $i ), $tmp += bytes::length( $_.'' ), ++$len ) for $i..10;
    @arr = $coll->validate;
    is $arr[0], $tmp,   "OK length";
    is $arr[1], $i,     "OK lists";
    is $arr[2], $len,   "OK items";
}

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

# Remove old data (insert)
$coll = Redis::CappedCollection->new(
    $redis,
    size    => 5,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;
$status_key  = NAMESPACE.':status:'.$coll->name;

$list_key = NAMESPACE.':D:*';
foreach my $i ( 1..( $coll->size * 2 ) )
{
    $id = $coll->insert( '*', $i );
    @arr = $coll->validate;
    is $arr[0], ( $i <= $coll->size ) ? $i : $coll->size, "OK length";
    is $arr[1], ( $i <= $coll->size ) ? $i : $coll->size, "OK lists";
    is $arr[2], ( $i <= $coll->size ) ? $i : $coll->size, "OK items";
}

$id = $coll->insert( '*' x $coll->size );
$tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
@arr = $coll->_call_redis( "KEYS", $list_key );
is $tmp, $coll->size, "correct length value";
is scalar( @arr ), 1, "correct lists value";

@arr = $coll->validate;
is $arr[0], $coll->size,    "OK length";
is $arr[1], 1,              "OK lists";
is $arr[2], 1,              "OK items";

dies_ok { $id = $coll->insert( '*' x ( $coll->size + 1 ) ) } "expecting to die";

@arr = $coll->validate;
is $arr[0], $coll->size,    "OK length";
is $arr[1], 1,              "OK lists";
is $arr[2], 1,              "OK items";

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

# limited size (update)
$coll = Redis::CappedCollection->new(
    $redis,
    size    => 10,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;

$status_key  = NAMESPACE.':status:'.$coll->name;
$queue_key   = NAMESPACE.':queue:'.$coll->name;
ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";

$coll->insert( $_, "id" ) for 1..9;
$list_key = NAMESPACE.':D:'.$coll->name.':id';
is $coll->_call_redis( "HLEN", $list_key ), 9, "correct list length";
is $coll->_call_redis( "HGET", $status_key, 'length' ), 9, "correct length value";

@arr = $coll->validate;
is $arr[0], 9, "OK length";
is $arr[1], 1,  "OK lists";
is $arr[2], 9,  "OK items";

$tmp = 0;
# 9 = 1  2  3  4  5  6  7  8  9
foreach my $i ( 1..9 )
{
    $tmp = $coll->update( "id", $i - 1, "$i*" );
    if ( $i == 1 )
    {
        # 10 = 1* 2  3  4  5  6  7  8  9
        ok $tmp, "OK update $i";
        @arr = $coll->validate;
        is $arr[0], 10, "OK length";
        is $arr[1], 1,  "OK lists";
        is $arr[2], 9,  "OK items";
    }
    elsif ( $i == 2 )
    {
        # 9 = 2* 3  4  5  6  7  8  9
        ok $tmp, "OK update $i";
        @arr = $coll->validate;
        is $arr[0], 9, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 8, "OK items";
    }
    elsif ( $i == 3 )
    {
        # 10 = 2* 3* 4  5  6  7  8  9
        ok $tmp, "OK update $i";
        @arr = $coll->validate;
        is $arr[0], 10, "OK length";
        is $arr[1], 1,  "OK lists";
        is $arr[2], 8,  "OK items";
    }
    elsif ( $i == 4 )
    {
        # 9 = 3* 4* 5  6  7  8  9
        ok $tmp, "OK update $i";
        @arr = $coll->validate;
        is $arr[0], 9, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 7, "OK items";
    }
    elsif ( $i == 5 )
    {
        # 10 = 3* 4* 5*  6  7  8  9
        ok $tmp, "OK update $i";
        @arr = $coll->validate;
        is $arr[0], 10, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 7, "OK items";
    }
    elsif ( $i == 6 )
    {
        # 9 = 4* 5* 6*  7  8  9
        ok $tmp, "OK update $i";
        @arr = $coll->validate;
        is $arr[0], 9, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 6, "OK items";
    }
    elsif ( $i == 7 )
    {
        # 10 = 4* 5* 6* 7*  8  9
        ok $tmp, "not updated $i";
        @arr = $coll->validate;
        is $arr[0], 10, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 6, "OK items";
    }
    elsif ( $i == 8 )
    {
        # 9 = 5* 6* 7* 8*  9
        ok $tmp, "not updated $i";
        @arr = $coll->validate;
        is $arr[0], 9, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 5, "OK items";
    }
    elsif ( $i == 9 )
    {
        # 10 = 5* 6* 7* 8* 9*
        ok $tmp, "not updated $i";
        @arr = $coll->validate;
        is $arr[0], 10, "OK length";
        is $arr[1], 1, "OK lists";
        is $arr[2], 5, "OK items";
        last;
    }
}

$tmp = $coll->update( "bad_id", 0, '*' );
ok !$tmp, "not updated";
@arr = $coll->validate;
is $arr[0], 10, "OK length";
is $arr[1], 1, "OK lists";
is $arr[2], 5, "OK items";

$tmp = $coll->update( "id", 0, '***' );
ok !$tmp, "not updated";
@arr = $coll->validate;
is $arr[0], 10, "OK length";
is $arr[1], 1, "OK lists";
is $arr[2], 5, "OK items";

}
