#!/usr/bin/perl
use 5.010;
use warnings;
use strict;

use Date::Parse;

my $MIN_DISPLAYED_DURATION = 0.040; # sec

my ( $tm_start, $tm_finish, $prev_script, $operation );
while ( <> ) {
    my $line = $_;

    if ( $line =~ / lua-script (start|finish): (\S+)/ ) {
        my $mode = $1;
        my $script = $2;
        next if $script eq 'upsert';

        ++$operation;
        my $tm = get_time( $line );
        if ( $mode eq 'start' ) {
            $tm_start = $tm;
            $prev_script = $script;
        } elsif ( defined( $tm_start ) && $script eq $prev_script && $mode eq 'finish' ) {
            $tm_finish = $tm;
            my $duration = $tm_finish - $tm_start;
            if ( $duration >= $MIN_DISPLAYED_DURATION ) {
                chomp $line;
                say sprintf( "operation = %d, '%s' duration: %.3f, finish line = '%s'",
                    $operation,
                    $script,
                    $duration,
                    $line,
                );
            }
            undef $tm_start;
            undef $prev_script;
        } else {
            chomp $line;
            warn sprintf( "not synchronized line: '%s', tm_start = %s, prev_script = '%s'",
                $line,
                $tm_start // '<undef>',
                $prev_script // '<undef>',
            );
        }
    }
}

sub get_time {
    my ( $line ) = @_;

    my ( $tm_str, $tm_ms ) = $line =~ / (\d\d \S\S\S \d\d:\d\d:\d\d)\.(\d\d\d) /;
    return str2time( $tm_str ) + $tm_ms / 1_000;
}