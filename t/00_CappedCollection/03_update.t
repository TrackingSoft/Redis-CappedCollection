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
    plan skip_all => "because Test::TCP required for testing" if $@;
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

#-- all correct

# some inserts
for ( my $i = 1; $i <= 10; ++$i )
{
    $coll->insert( $_, $i ) for $i..10;
}

# verify
for ( my $i = 1; $i <= 10; ++$i )
{
    foreach my $type ( qw( I D T ) )
    {
        $list_key = NAMESPACE.":$type:".$coll->name.':'.$i;
        ok $coll->_call_redis( "EXISTS", $list_key ), "data list created";
    }
    $list_key = NAMESPACE.':D:'.$coll->name.':'.$i;
    is( $coll->_call_redis( "HGET", $list_key, $_ - $i ), $_, "correct inserted value ($i list)" ) for $i..10;
}

# reverse updates
for ( my $i = 1; $i <= 10; ++$i )
{
    $tmp = $coll->update( $i, $_ - $i, 10 - $_ + $i ) for $i..10;
    ok $tmp, "correct update";
}

# verify
for ( my $i = 1; $i <= 10; ++$i )
{
    $list_key = NAMESPACE.':D:'.$coll->name.':'.$i;
    is( $coll->_call_redis( "HGET", $list_key, $_ - $i ), 10 - $_ + $i, "correct updated value ($i list)" ) for $i..10;
}

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

#-- resizing

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
$tmp = 0;
for ( my $i = 1; $i <= 10; ++$i )
{
    ( $coll->insert( $_, $i ), $tmp += bytes::length( $_."" ) ) for $i..10;
}

is $coll->_call_redis( "HGET", $status_key, 'length' ), $tmp, "correct length value";

# updates with resizing
$tmp = 0;
for ( my $i = 1; $i <= 10; ++$i )
{
    ( $coll->update( $i, $_ - $i, ( 10 - $_ + $i ).'*' ), $tmp += bytes::length( ( 10 - $_ + $i ).'*' ) ) for $i..10;
}

is $coll->_call_redis( "HGET", $status_key, 'length' ), $tmp, "correct length value";

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

# limited size
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

$tmp = 0;
# 9 = 1  2  3  4  5  6  7  8  9
foreach my $i ( 1..9 )
{
    $tmp = $coll->update( "id", $i - 1, "$i*" );
    if ( $i == 1 )
    {
        # 10 = 1* 2  3  4  5  6  7  8  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 10";
    }
    elsif ( $i == 2 )
    {
        # 9 = 2* 3  4  5  6  7  8  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 9, "correct length value 9";
    }
    elsif ( $i == 3 )
    {
        # 10 = 2* 3* 4  5  6  7  8  9
        ok $tmp, "OK update $i";
        $tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
        is $tmp, 10, "correct length value 10";
    }
    elsif ( $i == 4 )
    {
        # 9 = 3* 4* 5  6  7  8  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 9, "correct length value 9";
    }
    elsif ( $i == 5 )
    {
        # 10 = 3* 4* 5*  6  7  8  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 10";
    }
    elsif ( $i == 6 )
    {
        # 9 = 4* 5* 6*  7  8  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 9, "correct length value 10";
    }
    elsif ( $i == 7 )
    {
        # 10 = 4* 5* 6* 7*  8  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 10";
    }
    elsif ( $i == 8 )
    {
        # 9 = 5* 6* 7* 8*  9
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 9, "correct length value 10";
    }
    elsif ( $i == 9 )
    {
        # 10 = 5* 6* 7* 8* 9*
        ok $tmp, "OK update $i";
        is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 10";
        is $coll->_call_redis( "HLEN", $list_key ), 5, "correct list length";
        last;
    }
}

$tmp = $coll->update( "bad_id", 0, '*' );
ok !$tmp, "not updated";
is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 10";
is $coll->_call_redis( "HLEN", $list_key ), 5, "correct list length";

$tmp = $coll->update( "id", 3, '***' );
ok !$tmp, "not updated";
is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 8";
is $coll->_call_redis( "HLEN", $list_key ), 5, "correct list length";

$tmp = $coll->update( "id", 5-1, '***' );
ok !$tmp, "not updated";
# 8 = 6* 7* 8* 9*
is $coll->_call_redis( "HGET", $status_key, 'length' ), 8, "correct length value 8";
is $coll->_call_redis( "HLEN", $list_key ), 4, "correct list length";

$tmp = $coll->update( "id", 9-1, '*' x 10 );
ok $tmp, "updated";
# 10 = **********
is $coll->_call_redis( "HGET", $status_key, 'length' ), 10, "correct length value 8";
is $coll->_call_redis( "HLEN", $list_key ), 1, "correct list length";

# errors in the arguments
# 8 = 4* 6  5* 6* 9

dies_ok { $coll->update() } "expecting to die - no args";

foreach my $arg ( ( undef, "", \"scalar", [], $uuid ) )
{
    dies_ok { $coll->update(
        $arg,
        0,
        '*',
        ) } "expecting to die: ".( $arg || '' );
}

foreach my $arg ( ( undef, 9999999999999999, \"scalar", [], $uuid ) )
{
    dies_ok { $coll->update(
        'id',
        0,
        $arg,
        ) } "expecting to die: ".( $arg || '' );
}

foreach my $arg ( ( undef, "", \"scalar", [], $uuid ) )
{
    dies_ok { $coll->update(
        'id',
        $arg,
        '*',
        ) } "expecting to die: ".( $arg || '' );
}

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
