#!/usr/bin/perl
# Test Loading sets (AoH) and individual entries (hashes)
# Depends on DBI CSV file driver (DBD::CSV)
use Test::More tests => 5;
use Data::Dumper;
use StoredHash;
use DBI;

$Data::Dumper::Indent = 0;
our $dbh;

#$dbh = DBI->connect("DBI:CSV:f_dir=t");
$dbh = DBI->connect(qq{DBI:CSV:csv_sep_char=\\;;csv_eol=\n;});
$dbh or die "Cannot connect: " . $DBI::errstr;

setuptables($dbh);

my $sh = StoredHash->new('table' => 'anim', pkey => ['id'], 'dbh' => $dbh);
my $arr = $sh->loadset();
print(Dumper($arr));
ok (ref($arr) eq 'ARRAY', "Got a set of All Entries");
my $e = $sh->load([2]);
#print(Dumper($e));
ok (ref($e) eq 'HASH', "Got an Entry");
$sh->{'debug'} = 1;
$arr = $sh->loadset({'description' => '%Fur%',});
ok(@$arr == 2, "Got 2 Furry Animals");
$arr = $sh->loadset({'family' => 'mammal',});
ok(@$arr == 3, "Got 3 Mammals");
#print(Dumper($arr));
ok (ref($arr) eq 'ARRAY', "Got a Filtered set of Entries");

sub setuptables {
   my ($dbh) = @_;
   our $dir = (-f "anim.txt") ? "." : "t";
   my $fname = "$dir/anim.txt";
   if (!-f $fname) {die("No File $fname");}
   my $fname2 = "$dir/animfamily.txt";
   if (!-f $fname2) {die("No File $fname2");}
   $dbh->{'csv_tables'}->{'anim'} = {'file' => $fname};
   $dbh->{'csv_tables'}->{'animfamily'} = {'file' => $fname2};
}
