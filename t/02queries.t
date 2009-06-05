#!/usr/bin/perl
#use Test::Simple tests => 3;
#use Test::More tests => 3;
use Test::More tests => 1;
use Data::Dumper;
$Data::Dumper::Indent = 0;
use lib '..';
use StoredHash;
my $ent = {'brand' => 'Mercury', 'cycle' => 2, 'power' => 300,};
my $p = StoredHash->new('table' => 'Motors', 'pkey' => ['id'],);

makequeries($p, $ent, [32]);

my $ent2 = {'name' => 'Bill Hill', 'ctry' => 31, 'ssn' => 19857354,};
my $p2 = StoredHash->new('table' => 'People', 'pkey' => ['ctry','ssn',],);
makequeries($p2, $ent2, [31,'19857354',]);
ok(1, "Made a set of queries");
sub makequeries {
 my ($p, $ent, $idvs) = @_;
 my @vals = StoredHash::allentvals($ent);
 print(Dumper(\@vals)."\n");
 my $qi = $p->insert($ent);
 my $qu = $p->update($ent, $idvs);
 my $qe = $p->exists($idvs);
 my $ql = $p->load($idvs);
 my $qd = $p->delete($ent, $idvs);
 my @queries = ($qi, $qu, $qe, $ql, $qd);
 print(map({"$_;\n";} @queries), "\n\n");
 
}
