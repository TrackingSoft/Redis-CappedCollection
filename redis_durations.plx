#!/usr/bin/perl
use 5.010;
use warnings;
use strict;

use Date::Parse;

my $MIN_DISPLAYED_DURATION = 0.040; # sec

my ( $tm_start, $tm_finish, $prev_script, $operation, $start_line, @cleaned );
while ( <> ) {
    my $line = $_;

    if (
               $line =~ / lua-script (start|finish): (\S+)/
            || $line =~ / cleaned: (\d+) bytes, (\d+) items'/
        ) {
        my $first_val   = $1;
        my $second_val  = $2;
        next if $second_val eq 'upsert';

        ++$operation;
        my $tm = get_time( $line );
        if ( $first_val eq 'start' ) {
            $tm_start = $tm;
            $prev_script = $second_val;
            $start_line = $line;
            @cleaned = ();
        } elsif ( defined( $tm_start ) && $second_val eq $prev_script && $first_val eq 'finish' ) {
            $tm_finish = $tm;
            my $duration = $tm_finish - $tm_start;
            if ( $duration >= $MIN_DISPLAYED_DURATION ) {
                chomp $line;
                say sprintf( "operation = %d, '%s' duration: %.3f,\n\tstart line = %s\n\tfinish line = '%s'",
                    $operation,
                    $second_val,
                    $duration,
                    $start_line,
                    $line,
                );
                if ( @cleaned ) {
                    say "\n\tprevious cleaned:";
                    say "\n\t$_" foreach @cleaned;
                }
            }
            undef $tm_start;
            undef $prev_script;
            @cleaned = ();
        } elsif ( $first_val =~ /^\d+$/ ) {
            push @cleaned, $line;
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