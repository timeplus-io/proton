#!/usr/bin/env perl
use v5.14;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;

use Cwd qw/getcwd realpath/;
use File::Temp qw/tempdir/;
use List::Util qw/first/;

# system() wrapper to die with an error if a command fails.
sub _try_run {
    my @command = @_;
    say "> Running: @command";
    system(@command);
    die "Error running '@command" if $?;
}

# Quick replacement for File::pushd; see LocalGuard at end of this file.
sub _pushd {
    my $dir = shift;
    my $cwd = getcwd();
    my $guard = LocalGuard->new( sub { chdir( $cwd ) or die "$!" } );
    chdir( $dir ) or die $!;
    return $guard;
}

# Map git refs to standardized version names.
sub _ref_to_name {
    my $ref = shift;
    if ( substr($ref,0,1) eq 'r' ) {
        $ref =~ s/^r/mongocxx-/;
    }
    if ( $ref =~ /26compat/ ) {
        $ref =~ s/^.*26compat/26compat/;
    }
    return $ref;
}

# Doxygen config was stored in different places at times.
sub _find_doxygen_config {
    my $config = first { -f } qw( Doxyfile etc/doxygen/config doxygenConfig );
    die "Doxygen config not found!\n" unless $config;
    return $config;
}

# Utility for reading all file contents.
sub _slurp {
    my $file = shift;
    open my $fh, "<:encoding(UTF-8)", $file
        or die "Error opening '$file' to read: $!\n";
    local $/;
    return scalar <$fh>;
}

# Utility for writing all file contents.
sub _spew {
    my $file = shift;
    open my $fh, ">:encoding(UTF-8)", $file
        or die "Error opening '$file' to write: $!\n";
    print {$fh} @_;
    return;
}

# Parse doxygen config into a hash with key and full line
sub _parse_doxygen_config {
    my $file = _find_doxygen_config();
    open my $fh, "<:encoding(UTF-8)", $file
        or die "Error opening '$file': $!\n";
    my %config;
    while ( my $line = <$fh> ) {
        # Skip comment lines and lines that aren't assignments
        next if substr($line,0,1) eq '#';
        next unless $line =~ /\S.*=/;
        # Join lines ending in backslash.
        while ( $line =~ s{\\\n\z}{} ) {
            $line .= " " . <$fh>;
        }
        # Save full line under the assigned key
        my ($key) = $line =~ m{\s*(\S+?)\s*=};
        $config{$key} = $line;
    }
    return \%config;
}

sub main {
    my $gitref = shift @ARGV;

    die "Usage: $0 <git-reference>\n" unless defined $gitref;
    die "Must run from top of the repo\n" unless -d ".git";

    my $orig_dir = getcwd();
    my $version = _ref_to_name($gitref) // 'current';
    my $out_dir = "$orig_dir/build/docs/api/$version";

    # Create tempdir to store copy of repo.  This shouldn't be
    # /tmp because on OS X that has a realpath of "/private/tmp",
    # which trips the EXCLUDE_PATTERN of *private*.
    _try_run("mkdir", "-p", "build/tmp-repo");
    my $tmp = tempdir( DIR => "build/tmp-repo", CLEANUP => 1 );
    my $tmpdir = realpath("$tmp");

    # Clone current repo to tempdir and checkout target tag.
    _try_run(qw/git clone . /, $tmpdir);
    my $guard = _pushd($tmpdir);
    _try_run(qw/git checkout/, $gitref);

    # Parse doxygen config
    my $config = _parse_doxygen_config();

    # Remove default apidocmenu so it doesn't conflict with the
    # new one we'll generate later.
    $config->{INPUT} =~ s{etc/apidocmenu\.md}{};

    # Create output directory
    say "Making '$out_dir'";
    _try_run(qw/mkdir -p/, $out_dir);

    # Copy front matter
    _spew( "$tmpdir/apidocmenu.md", _slurp("$orig_dir/etc/apidocmenu.md") );

    # Construct new doxygen file from current Doxygen config
    _spew( "$tmpdir/Doxyfile",
        _slurp("$orig_dir/Doxyfile"),
        $config->{INPUT},
        $config->{EXCLUDE},
        qq[INPUT += apidocmenu.md\n],
        qq[FILE_PATTERNS += *.h\n],
        qq[EXCLUDE += README.md CONTRIBUTING.md TODO.md\n],
        qq[USE_MDFILE_AS_MAINPAGE = apidocmenu.md\n],
        qq[PROJECT_NUMBER = "$version"\n],
        qq[OUTPUT_DIRECTORY = "$out_dir"\n],
        qq[HTML_EXTRA_STYLESHEET = "$orig_dir/etc/doxygen-extra.css"\n],
    );

    # Run doxygen
    _try_run('doxygen', "$tmpdir/Doxyfile");
}

main();

# Quick replacement for Scope::Guard.
package LocalGuard;

sub new {
    my ($class, $code) = @_;
    return bless { demolish => $code}, $class;
}

sub DESTROY {
    my $self = shift;
    $self->{demolish}->() if $self->{demolish};
}

