#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 22;

BEGIN { use_ok 'Redis::CappedCollection' }

can_ok( 'Redis::CappedCollection', $_ ) foreach qw(
    new
    insert
    update
    receive
    collection_info
    info
    pop_oldest
    exists
    lists
    drop_collection
    drop
    quit

    max_datasize
    last_errorcode
    name
    size
    advance_cleanup_bytes
    advance_cleanup_num
    older_allowed
    big_data_threshold
    );

my $val;
ok( $val = Redis::CappedCollection::MAX_DATASIZE, "import OK: $val" );
