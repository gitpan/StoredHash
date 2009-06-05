#use Test::Simple tests => 3;
use Test::More tests => 3;
use lib '..';
use StoredHash;
#use Scalar::Util ('reftype');

ok(1, "Module load OK");

ok($StoredHash::VERSION, "Module Has VERSION String");
my $p = StoredHash->new('pkey' => ['id'],);
#if( $^O eq 'MacOS' ) {}

ok(ref($p) eq 'StoredHash', "Got Instance of persister");
