#!/usr/bin/env perl
use v5.14;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;

use Cwd qw/getcwd realpath/;
use File::Temp qw/tempdir/;
use List::Util qw/first/;
use Getopt::Long;

# system() wrapper to die with an error if a command fails.
sub _try_run {
    my @command = @_;
    say "> Running: @command";
    system(@command);
    die "Error running '@command" if $?;
}

# Quick replacement for File::pushd; see LocalGuard at end of this file.
sub _pushd {
    my $dir   = shift;
    my $cwd   = getcwd();
    my $guard = LocalGuard->new( sub { chdir($cwd) or die "$!" } );
    chdir($dir) or die $!;
    return $guard;
}

sub _hugo_rsync {
    my $tmpdir = shift;
    _try_run( qw{rsync -Cavz --delete --exclude=/api --exclude=/.git* --exclude=CNAME build/hugo/},
        $tmpdir );
}

sub _doxygen_rsync {
    my $tmpdir = shift;
    my @filters = ( '- /current', '- /mongocxx-v3', '- /legacy-v1' );
    _try_run(
        qw{rsync -Cavz --delete},
        ( map { ; '--filter' => $_ } @filters ),
        "build/docs/api/", "$tmpdir/api/"
    );
}

sub main {
    die "Must run from top of the repo\n" unless -d ".git";

    my ( $do_hugo, $do_doxygen );
    GetOptions( hugo => \$do_hugo, doxygen => \$do_doxygen );
    my $source_repo = shift @ARGV;

    die "Usage: $0 <--hugo|--doxygen> <repo source>\n"
      unless defined $source_repo && ( $do_hugo || $do_doxygen );

    my $orig_dir = getcwd();

    # Create tempdir to store copy of repo.
    my $tmp = tempdir( DIR => "/tmp", CLEANUP => 1 );
    my $tmpdir = realpath("$tmp");

    # Clone current repo to tempdir and checkout gh-pages branch.
    _try_run( qw/git clone/, $source_repo, $tmpdir );
    {
        my $guard = _pushd($tmpdir);
        _try_run(qw/git checkout gh-pages/);
    }

    # rsync files to target repo pages based on command line flags.
    $do_hugo ? _hugo_rsync($tmpdir) : _doxygen_rsync($tmpdir);

    # Check into git and deploy
    COMMIT: {
        my $guard = _pushd($tmpdir);
        # Check in changes -- wow this is so insanely destructive; good
        # thing it's a version control system!
        _try_run(qw/git add --all ./);
        _try_run(qw/git status/);

        # Can exit if no changes were made.
        if ( eval { _try_run(qw/git diff-index --quiet --cached HEAD/); 1 } ) {
            say "No changes to deploy.";
        }
        else {
            _try_run( qw/git commit -m/,
                $do_hugo ? "Update hugo files" : "Update doxygen files" );
            _try_run(qw/git push origin gh-pages/);
        }
    }
}

main();
exit 0;

# Quick replacement for Scope::Guard.
package LocalGuard;

sub new {
    my ( $class, $code ) = @_;
    return bless { demolish => $code }, $class;
}

sub DESTROY {
    my $self = shift;
    $self->{demolish}->() if $self->{demolish};
}

