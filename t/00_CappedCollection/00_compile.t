#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

BEGIN { use_ok 'Redis::CappedCollection' }

can_ok( 'Redis::CappedCollection', $_ ) foreach qw(
    new
    create
    insert
    update
    receive
    collection_info
    list_info
    oldest_time
    pop_oldest
    list_exists
    lists
    drop_collection
    clear_collection
    drop_list
    ping
    quit

    max_datasize
    last_errorcode
    name
    advance_cleanup_bytes
    advance_cleanup_num
    older_allowed
    );

my $val;
ok( $val = $Redis::CappedCollection::MAX_DATASIZE, "import OK: $val" );
