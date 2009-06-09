# Minimalistic, yet fairly complete DBI Persister
# Allow DB Persistence operations (insert(), load(), update(), delete(),
# exists()) on a plain old hash (unblessed or blessed) without writing
# classes, persistence code or SQL.

# Author: olli.hollmen@gmail.com
# License: Perl License

# StoredHash needs an OO instance of persister to function.

# Because insert, update (the vals we want to pers.) are instance specific
# Possibly return an object or bare hash from preparation of ins/upd/del
# With 
# - query
# - vals (to pass to exec)
# - attr (needed ?)
# - Assigned ID ?
#  Make this object w. meth execute() ???? getid()

# TODO: Change/Add pkey => idattr @pkv => @idv
# Support Mappings (before storage as separate op ?)
package StoredHash;
use Scalar::Util ('reftype'); # 
use Data::Dumper;

#use strict;
#use warnings;
our $VERSION = '0.029';
# Module extraction config
our $mecfg = {};
# Instance attr (create access ...)
# Allow 'attr' to act as attr filter
my @opta = ('dbh', 'table','pkey','autoid','autoprobe','simu','errstr',
   'seqname','debug',); # 
# TODO: Support sequence for Oracle / Postgres
# seq_emp.NEXTVAL
my $bkmeta = {
   #'mysql'  => {'iq' => "SELECT LAST_INSERT_ID()",},
   #'Sybase' => {'iq' => "SELECT \@\@identity",},
   'Oracle' => {
      #'iq' => "SELECT \@\@identity",
      'sv' => '%s.NEXTVAL',}, # AS adid SET NOCOUNT OFF
   # Postgres ???
};

# Create New instance of StoredHash Persister.
# Options in %opt must have
# - pkey/idattr - array (ref) to reflect the identifying attrtibute(s) of
#   entry (single attr for numeric ids, multiple for composite key)
# Optional attributes
# - dbh - DBI connection to database. Not passing 'dbh' makes
#   methods insert/update/load/delete return the SQL query only (as a string)
sub new {
   my ($class, %opt) = @_;
   my $self = {};
   
   # Generate where by pkey OR use where
   #if ($opt{'where'}) {}
   # Moved for early bless
   bless($self, $class);
   # For Child loading / temp use
   if ($opt{'loose'}) {goto PASSPKEY;}
   if ($opt{'pkey'}) {
      $self->{'pkey'} = $opt{'pkey'};
      # TODO: Do NOT cache WHERE id ...
      $self->{'where'} = whereid($self); # \%opt # join('AND', map({" $_ = ?";} pkeys(\%opt));
   }
   else {die("Need pkey info");}
   PASSPKEY:
   # Validate seq. (Need additional params to note call for seq?)
   #if ($opt{'autoid'} eq 'seq') {
   #   #$c{'seqcall'};
   #}
   # Filter options to self
   @$self{@opta} = @opt{@opta};
   
   return($self);
}
# Access error string that method may leave to object.
# Notice that many methods throw exception (by die()) with
# error message rather than leave it within object.
sub errstr {
   my ($p, $v) = @_;
   if ($v) {$p->{'errstr'} = $v;}
   $p->{'errstr'};
}

# Internal method for executing query $q by filling placeholders with
# values passed in @$vals.
# Optional $rett (usually not passed) can force a special return type
# Some supported return force tags:
# - 'count' - number of entries counted with count(*) query
# - 'sth' - return statement handle ($sth), which will be used outside.
# - 'hash' - return a hash entry (first entry of resultset)
# - 'aoh'  - return array of hashes reflecting result set.
# By default (no $rett) returns the ($ok)value from $sth->execute().
# Also by default statement statement handle gets properly closed
# (If requested return type was $sth, the caller should take care of
# calling $sth->finish()
sub qexecute {
   my ($p, $q, $vals, $rett) = @_;
   my $dbh = $p->{'dbh'};
   my $sth; # Keep here to have avail in callbacks below
   if (!$dbh || $p->{'simu'}) { # 
      local $Data::Dumper::Terse = 1;
      local $Data::Dumper::Indent = 0;
      print("SQL($p->{'table'}): $q\nPlaceholder Vals:".Dumper($vals)."\n");
      return(0);
   }
   # Special Return value generators
   # These should also close the statement (if that is not returned)
   my $rets = {
      'count' => sub {my @a = $sth->fetchrow_array();$sth->finish();$a[0];},
      'sth'   => sub {return($sth);},
      'hash'  => sub {my $h = $sth->fetchrow_hashref();$sth->finish();$h;},
      'aoh'   => sub {my $arr = $sth->fetchall_arrayref({});$sth->finish();$arr;},
   };
   if (!$dbh) {$p->{'errstr'} = "No Connection !";return(0);}
   if ($p->{'debug'}) {print("Full Q: $q\n");}
   # Prepare cached ?
   $sth = $dbh->prepare($q);
   if (!$sth) {die("Query ($q) Not prepared (".$dbh->errstr().")\n");}
   my $ok = $sth->execute(@$vals);
   if (!$ok) {die("Failed to execute ".$sth->errstr()."");}
   # Special return processing
   if (my $rcb = $rets->{$rett}) {
      #print("Special return by $rett ($rcb)\n");
      return($rcb->());
   }
   # Done with statement
   DWS:
   $sth->finish();
   return($ok);
}

###################################################

# Store entry %$e (hash) inserting it as a new entry to a database.
# Connection has been passed previously in construction of persister.
# The table / schema to store to is either the one passed at
# construction or derived from perl "blessing" of entry ($e).
# Returns (ref to) an array of ID values for the entry that got stored (array
# of one element for numeric primary key, multiple for composite key).
sub insert {
   my ($p, $e) = @_;
   # No enforced internal validation
   eval {$p->validate();};if ($@) {$p->{'errstr'} = $@;return(1);}
   if (reftype($e) ne 'HASH') {$p->errstr("Entry need to be HASH");return(2);}
   # Possibly also test for references (ds branching ?) eliminating them too
   my @ea = sort (keys(%$e));
   my @ev = @$e{@ea}; # map()
   
   # Sequence - Add sequenced ID allocation ???
   # $p->{'seqname'}
   if ($p->{'autoid'} && ($p->{'autoid'} eq 'seq')) {
      my $bkt = 'Oracle';
      my @pka = pkeys($p);
      if (@pka > 1) {die("Multiple pkeys for sequenced ID");}
      # Add Sequence id attibute AND sequence call (unshift to front ?)
      # 
      push(@ea, @pka); #  $p->{'pkey'}->[0]
      push(@ev, sprintf("$bkmeta->{$bkt}->{'sv'}", $p->{'seqname'}) ); # 
      #DEBUG:print("FMT: $bkmeta->{$bkt}->{'sv'} / $p->{'seqname'}\n");
   }
   my $qp = "INSERT INTO $p->{'table'} (".join(',',@ea).") ".
      "VALUES (".join(',', map({'?';} @ea)).")";
   if (!$p->{'dbh'}) {return($qp);}
   my $okid = $p->qexecute($qp, \@ev);
   
   # Auto-id - either UTO_INC style or Sequence (works for seq. too ?
   if ($p->{'autoid'}) {
      my @pka = pkeys($p);
      if (@pka != 1) {die(scalar(@pka)." Keys for Autoid");}
      my $id = $p->fetchautoid();
      #$e->{$pka[0]} = $id;
      return(($id));
   }
   # Seq ?
   #elsif () {}
   else {
      my @pka = pkeys($p);
      return(@$e{@pka});
   }
}

# Update an existing entry in the database with values in %$e (hash).
# Provide protection for AUTO-ID (to not be changed) ?
# For flexibility the $idvals may be hash or array (reference) with
# hash containing (all) id keys and id values or alternatively array
# containing id values IN THE SAME ORDER as keys were passed during
# construction (with idattr/pkey parameter).
sub update {
   my ($p, $e, $idvals) = @_;
   my @pka; # To be visible to closure
   # Extract ID Values from hash OR array
   my $idvgens = {
      'HASH'  => sub {@$idvals{@pka};},
      'ARRAY' => sub {return(@$idvals);},
      #'' => sub {[$idvals];}
   };
   # No mandatory (internal) validation ?
   #eval {$p->validate();};if ($@) {$p->{'errstr'} = $@;return(1);}
   @pka = pkeys($p);
   if (reftype($e) ne 'HASH') {$p->{'errstr'} = "Entry need to be hash";return(2);}
   # Probe the type of $idvals
   my $idrt = reftype($idvals);
   if ($p->{'debug'}) {print("Got IDs:".Dumper($idvals)." as $idrt\n");}
   #my @idv;
   my @pkv;
   if (my $idg = $idvgens->{$idrt}) {@pkv = $idg->();}
   #if ($idrt ne 'HASH') {$p->{'errstr'} = "ID needs to be hash";return(3);}
   else {die("Need IDs as HASH or ARRAY (reference, got '$idrt')");}
   #my ($cnt_a, $cnt_v) = (scalar(@pka), scalar(@pkv));
   if (@pkv != @pka) {die("Number of ID keys and values not matching for update");}
   my @ea = sort(keys(%$e));
   #my @pkv = @$idh{@pka}; # $idvals, Does not work for hash
   
   if (my @badid = $p->invalidids(@pkv)) {$p->{'errstr'} = "Bad ID Values found (@badid)";return(4);}
   my $widstr = whereid($p);
   # Persistent object type
   my $pot = $p->{'table'};
   if (!$pot) {die("No table for update");}
   my $qp = "UPDATE $pot SET ".join(',', map({" $_ = ?";} @ea)).
      " WHERE $widstr";
   if (!$p->{'dbh'}) {return($qp);}
   # Combine Entry attr values and primary key values
   my $allv = [@$e{@ea}, @pkv];
   $p->qexecute($qp, $allv);
}

# Delete an entry from database by passing $e as one of the following
# - hash %$e - a hash containing (all) primary key(s) and their values.
# - scalar $e - Entry ID for entry to be deleted
# - array @$e - One or many primary key values for entry to be deleted
# The recommended use is caae "array" as it is most versatile and most
# consistent with other API methods.
sub delete {
   my ($p, $e) = @_;
   #if (!ref($p->{'pkey'})) {die("PKA Not Known");}
   #eval {$p->validate();};if ($@) {$p->{'errstr'} = $@;return(1);}
   #my @pka = @{$p->{'pkey'}}; 
   my @pka = pkeys($p);
   if (!$e) {die("Must have Identifier for delete()\n");}
   
   my $rt = reftype($e);
   my $pkc = $p->pkeycnt();
   my @pkv;
   # $e Scalar, must have 1 pkey
   if (!$rt && ($pkc == 1)) {@pkv = $e;}
   # Hash - extract primary keys
   elsif ($rt eq 'HASH') {@pkv = @$e{@pka};}
   # Array (of pk values) - check count matches
   elsif (($rt eq 'ARRAY') && ($pkc == scalar(@$e))) {@pkv = @$e;}
   else {die("No way to delete (without HASH or ARRAY for IDs)\n");}
   #NOTNEEDED:#my %pkh;@pkh{@pka} = @pkv;
   #my $wstr = join(' AND ', map({"$_ = ?";} @pka));
   my $wstr = whereid($p);
   my $qp = "DELETE FROM $p->{'table'} WHERE $wstr";
   if (!$p->{'dbh'}) {return($qp);}
   $p->qexecute($qp, \@pkv);
}
#my $dbh = $p->{'dbh'};
#my $sth = $dbh->prepare($qp);
#if (!$sth) {print("Not prepared\n");}
#$sth->execute(@pkv);

# Test if an entry exists in the DB table with ID values passed in @$ids (array).
# Returns 1 (entry exists) or 0 (does not exist) under normal conditions.
sub exists {
   my ($p, $ids) = @_;
   my $whereid = $p->{'where'} ? $p->{'where'} : whereid($p);
   my $qp = "SELECT COUNT(*) FROM $p->{'table'} WHERE $whereid";
   if (!$p->{'dbh'}) {return($qp);}
   $p->qexecute($qp, $ids, 'count');
}

# Load entry from DB table by its IDs passed in @$ids (array, 
# single id typical sequece autoid pkey, multiple for composite primary key).
# Entry will be loaded from single table passed at construction
# (never as result of join from multiple tables).
# Return entry as a hash (ref).
sub load {
   my ($p, $ids) = @_;
   my $whereid = $p->{'where'} ? $p->{'where'} : whereid($p);
   my $qp = "SELECT * FROM $p->{'table'} WHERE $whereid";
   if (!$p->{'dbh'}) {return($qp);}
   $p->qexecute($qp, $ids, 'hash');
}

# Load a set of Entries from persistent storage.
# Optionally provide simple "where filter hash" ($h), whose key-value criteria
# is ANDed together to form the filter.
# Return set / collection of entries as array of hashes.
sub loadset {
   my ($p, $h, $sort) = @_; # filter, sortby
   my $w = '';
   # if (@_ = 2 && ref($_[1]) eq 'HASH') {}
   if ($h) {
      my $wf = wherefilter($h);
      if (!$wf) {die("Empty Filter !");}
      $w = " WHERE $wf";
   }
   if ($p->{'debug'}) {print("Loading set by '$w'\n");}
   my $qp = "SELECT * FROM $p->{'table'} $w";
   $p->qexecute($qp, undef, 'aoh');
}

# Sample Column names from (current) DB table.
# Return (ref to) array with field names in it.
sub cols {
   my ($p) = @_;
   my $qp = "SELECT * FROM $p->{'table'} WHERE 1 = 0";
   my $sth = $p->qexecute($qp, undef, 'sth');
   my $cols = $sth->{'NAME'};
   if (@_ == 1) {$sth->finish();return($cols);}
   #elsif (@_ == 2) {$rett = $_[1];};
   #if ($rett ne 'meta') {return(undef);}
   return(undef);
}

# TODO: Load "tree" of entries rooted at an entry / entries (?)
# Returns a set (array) of entries or single (root entry if
# option $c{'fsingle'} - force single - is set.
sub loadtree {
   my ($p, %c) = @_;
   my $chts = $c{'ctypes'};
   my $w = $c{'w'};
   my $fsingle = $c{'fsingle'}; # singleroot, uniroot
   my $arr = loadset($p, $w);
   for my $e (@$arr) {my $err = loadchildern($p, $e, %c);}
   # Choose return type
   if ($fsingle) {return($arr->[0]);}
   return($arr);
}

# TODO: Load Instances of child object types for entry.
# Child types are defined in 'ctypes' array(ref) in options.
# Array 'ctypes' may be one of the following
# - Plain child type names (array of scalars), the rest is guessed
# - Array of child type definition hashes with hashes defining following:
#   - table   - The table / objectspace of child type
#   - parkey  - Parent id field in child ("foreign key" field in rel DBs)
#   - memname - Mamber name to place the child collection into in parent entry
# - Array of arrays with inner arrays containing 'table','parkey','memname' in
#   that order(!), (see above for meanings)
# Return 0 for no errors
sub loadchildren {
  my ($p, $e, %c) = @_;
  my $chts = $c{'ctypes'};
  if (!$chts) {die("No Child types indicated");}
  if (ref($chts) ne 'ARRAY') {die("Child types not ARRAY");}
  my @ids = pkeyvals($p, $e);
  if (@ids > 1) {die("Loading not supported for composite keys");}
  my $dbh = $p->{'dbh'};
  my $debug = $p->{'debug'};
  for (@$chts) {
     #my $ct = $_;
     my $cfilter;
     # Use or create a complete hash ?
     my $cinfo = makecinfo($p, $_);
     if ($debug) {print(Dumper($cinfo));}
     # Load type by created filter
     my ($ct, $park, $memn) = @$cinfo{'table','parkey','memname',};
     if (!$park) {}
     # Create where by parkey info
     #$cfilter = {$park => $ids[0]}; # What is par key - assume same as parent
     if (@$park != @ids) {die("Par and child key counts mismatch");}
     @$cfilter{@$park} = @ids;
     #my $cfilter = 
     # Take a shortcut by not providing pkey
     my $shc = StoredHash->new('table' => $ct, 'pkey' => [],
        'dbh' => $dbh, 'loose' => 1, 'debug' => $debug);
     my $carr = $shc->loadset($cfilter);
     if (!$carr || !@$carr) {next;}
     #if ($debug) {print("Got Children".Dumper($arr));}
     $e->{$memn} = $carr;
     # Blessing
     if (my $bto = $cinfo->{'blessto'}) {map({bless($_, $bto);} @$carr);}
     # Circular Ref from child to parent ?
     #if (my $pla = $cinfo->{'parlinkattr'}) {map({$_->{$pla} = $e;} @$carr);}
  }
  # Autobless Children ?
  return(0);
}
# Internal method for using or making up Child relationship information
# for loading related entities.
sub makecinfo {
   my ($p, $cv) = @_;
   # Support array with: 'table','parkey','memname'
   if (ref($cv) eq 'ARRAY') {
      my $cinfo;
      if (@$cv != 3) {die("Need table, parkey, memname in array");}
      @$cinfo{'table','parkey','memname'} = @$cv;
      return($cinfo);
   }
   # Assume all is there (could validate and provide missing)
   elsif (ref($cv) eq 'HASH') {
      my @a = ('table','parkey','memname');
      # Try guess parkey ?
      if (!$cv->{'parkey'}) {$cv->{'parkey'} = [pkeys($p)];}
      for (@a) {if (!$cv->{$_}) {die("Missing '$_' in cinfo");}}
      return($cv);
   }
   elsif (ref($cv) ne '') {die("child type Not scalar (or hash)");}
   ################## Make up
   my $ctab = $cv;
   my $memname = $ctab; # Default memname to child type name (Plus 's') ?
   # Guess by parent
   my $parkey = [pkeys($p)];
   my $cinfo = {'table' => $ctab, 'parkey' => $parkey, 'memname' => $ctab,};
   return($cinfo);
}
###################################################################

# Internal Persister validator for the absolutely mandatory properties of
# persister object itself.
# Doesn't not validate entry
sub validate {
   my ($p) = @_;
   if (ref($p->{'pkey'}) ne 'ARRAY') {die("PK Attributes Not Known\n");}
   # Allow table to come from blessing (so NOT required)
   #if (!$p->{'table'}) {die("No Table\n");}
   if ($p->{'simu'}) {return;}
   # Do NOT Require conenction
   #if (!ref($p->{'dbh'})) {die("NO dbh to act on\n");} # ne 'DBI'
   
}

# Internal method for returning  array of id keys (Real array, not ref).
sub pkeys {
   my ($p) = @_;
   my $prt = reftype($p);
   if ($prt ne 'HASH') {
      $|=1;
      print STDERR Dumper([caller(1)]);
      die("StoredHash Not a HASH (is '$p'/'$prt')");
   }
   if (ref($p->{'pkey'}) ne 'ARRAY') {die("Primary keys not in an array");}
   #return($p->{'pkey'});
   return(@{$p->{'pkey'}});
}

# Return Primary key values (as real array) from hash %$e passed as parameter.
# undef values are produced for non-existing keys.
# Mostly used for internal operations (and maybe debugging).
sub pkeyvals {
   my ($p, $e) = @_;
   my @pkeys = pkeys($p);
   @$e{@pkeys};
}

# TODO: Implement pulling last id from sequence
sub fetchautoid {
   my ($p) = @_;
   my $dbh;
   #$dbh->{'Driver'}; # Need to test ?
   #DEV:print("AUTOID FETCH TO BE IMPLEMENTED\n");return(69);
   my $pot = $p->{'table'};
   if (!$pot) {die("No table for fetching auto-ID");}
   if (!($dbh = $p->{'dbh'})) {die("No Connection for fetching ID");}
   $dbh->last_insert_id(undef, undef, $pot, undef);
}

sub pkeycnt {
   my ($p) = @_;
   #if (ref($p->{'pkey'}) ne 'ARRAY') {die("Primary keys not in an array");}
   #scalar(@{$p->{'pkey'}});
   my @pkeys = pkeys($p);
   scalar(@pkeys);
}

# Internal method for checking for empty or undefined ID values.
# In all reasonable databases and apps these are not valid values.
sub invalidids {
   my ($p, @idv) = @_;
   my @badid = grep({!defined($_) || $_ eq '';} @idv);
   return(@badid);
}
# Generate SQL WHERE Clause for UPDATE based on primary keys of current type.
# Return WHERE clause with id-attribute(s) and placeholder(s) (idkey = ?, ...), without  the WHERE keyword.
sub whereid {
   my ($p) = @_;
   # # Allow IDs to be hash OR array ?? Not because hash would req. to store order
   my @pka = pkeys($p);
   if (@pka < 1) {die("No Pkeys to create where ID clause");}
   # my $wstr = 
   return join(' AND ', map({"$_ = ?";} @pka));
}

sub sqlvalesc {
   my ($v) = @_;
   $v =~ s/'/\\'/g;
   $v;
}

# TODO: Create list for WHERE IN Clause based on some assumptions
sub invalues {
   my ($vals) = @_;
   # Assume array ref validated outside
   if (ref($vals) eq 'ARRAY') {die("Not an array for invals");}
   # Escape within Quotes ?
   join(',', map({
      if (/^\d+$/) {$_;}
      else {
      my $v = sqlvalesc($_);
      "'$v'";
      }
   } @$vals));
}

sub rangefilter {
   my ($attr, $v) = @_;
   # Or just even and sort, grab 2 at the time ?
   if (@$v != 2) {die("Range cannot be formed");}
   # Auto-arrange ???
   #if ($v->[1] < $v->[0]) {$v = [$v->[1],$v->[0]];}
   # Detect need to escape (time vs. number)
   #"($attr >= $v->[0]) AND ($attr <= $v->[0])";
}

# Generate simple WHERE filter by hash %$e. The keys are assumed to be attributes
# of DB and values are embedded as values into SQL (as opposed to using placeholers).
# To be perfect in escaping per attribute type info would be needed.
# For now we do best effort heuristics (attr val \d+ is assumed
# to be a numeric field in SQL, however 000002345 could actually 
# be content of a char/text/varchar field).
# Return WHERE filter clause without WHERE keyword.
sub wherefilter {
   my ($e, %c) = @_;
   my $w = '';
   my $fop = ' AND ';
   #my $rnga = $c{'rnga'}; # Range attributes
   if (ref($e) ne 'HASH') {die("No hash for filter generation");}
   my @keys = sort keys(%$e);
   my @qc;
   # Assume hard values, treat everything as string (?)
   # TODO: forcestr ?
   @qc = map({
      my $v = $e->{$_};
      #my $rv = ref($v);
      #if ($rnga->{$_} && ($rv eq 'ARRAY') && (@$v == 2)) {rangefilter($_, $v);}
      if (ref($v) eq 'ARRAY') {" $_ IN (".invalues($v).") ";}
      # SQL Wildcard
      elsif ($v =~ /%/) {"$_ LIKE '$v'";}
      # Detect numeric (likely numeric, not perfect)
      elsif ($v =~ /^\d+$/) {"$_ = $v";}
      # Assume string
      else {"$_ = '".sqlvalesc($v)."'";}
      
   } @keys);
   return(join($fop, @qc));
}

# Internal: Serialize all values (singles,multi) from a hash to an array
# based on sorted key order. Multi-valued keys (with value being array reference)
# add multiple items. 
sub allentvals {
   my ($h) = @_;
   map({
      if (ref($h->{$_}) eq 'HASH') {();}
      elsif (ref($h->{$_}) eq 'ARRAY') {@{$h->{$_}};}
      else {($h->{$_});}
   } sort(keys(%$h)));
}
1;
