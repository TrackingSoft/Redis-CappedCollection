#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 15;

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

BEGIN { use_ok 'Redis::CappedCollection', qw(
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
    ) }

my $val;

ok( defined( $_ ), "import OK: $_" ) foreach qw(
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
