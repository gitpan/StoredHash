use ExtUtils::MakeMaker;
#
#require 5.6; # How to say this

WriteMakefile(
  'NAME'	 => 'StoredHash',
  'VERSION_FROM' => 'StoredHash.pm', # finds $VERSION
  'ABSTRACT'	 => 'Easy Insert, Update, Load and Delete of a DB (DBI) Stored Hash',
  'AUTHOR'	 => 'Olli Hollmen',
  # Class name => ver
  'PREREQ_PM'	 => {
      'DBI' => 1.40,
      'Data::Dumper' => 2.121,
  }
);
