#!/usr/bin/perl -w

use strict;

use DBI;
use Text::xSV;
use Data::Dumper::Simple;

my ($debug, $count);

#system( "/usr/bin/mysql -h warehouse.brec.local -u update -pupdating outage -e ' update account_list set status=2 '");
system( "/usr/bin/mysql --login-path=warehouse-update outage -e ' update account_list set status=2 '");

my $dbdriver = DBI->install_driver('mysql');

#my $dbi = DBI->connect( "DBI:mysql:outage;localhost;mysql_socket=/var/run/mysql/mysql.sock", "update", "updating" )
my $dbi = DBI->connect( "DBI:mysql:outage;warehouse.brec.local", "update", "[REDACTED]" )
    or die ( "Unable to connect to your database!" );

my $csv = new Text::xSV;
my $csv2 = new Text::xSV;
my @dbname_cases = ( 'city', 'state', 'status', 'rate_code', 'street_name', 'street_type',
                     'carrier_code', 'service_type' );
my @insert_cases = ( 'city', 'state', 'status', 'rate_code', 'name', 'type',
                     'carrier_code', 'service_type' );
my %entry_fields = ( mapno => 'MAPNO',
                     loctype_id => 'LOCTYPE_ID',
                     ats_id => 'LOCATION_ID',
                     line_and_pole => 'LINE_AND_POLE', 
                     meterno => 'METERNO', 
                     phase => 'PHASE', 
                     disconnect_date => 'DISCONNECT_DATE', 
                     reading_date => 'READING_DATE', 
                     customer_name => 'CUSTOMER_NAME', 
                     phone => 'PHONE', 
                     phone_ext => 'PHONE_EXT',
# This will soon be coming out!  Hooray!
# snip snip snip
#                    street_no => 'STREET_NO', 
#                    unit => 'UNIT', 
#                    street_name => 'STREET_NAME', 
#                    street_type => 'STREET_TYPE', 
#                    city => 'CITY', 
#                    state => 'STATE', 
#                    zip => 'ZIP', 
#                    zip_4 => 'ZIP_4', 
# snip snip snip
                     status => 'ACCT_STATUS', 
                     carrier_code => 'CARRIER_CODE', 
                     service_type => 'SERVICE_TYPE', 
                     rate_code => 'RATE_CODE'
                   );
my %atsid_fields = ( number => 'STREET_NO', 
                     unit => 'UNIT', 
                     name => 'STREET_NAME', 
                     type => 'STREET_TYPE', 
                     city => 'CITY', 
                     state => 'STATE', 
                     zip => 'ZIP', 
                     zip_4 => 'ZIP_4'
                   );
my $account_key = 'accountno';
my $account_record = 'ACCOUNTNO';
my $atsid_key = 'id';
my $atsid_record = 'LOCATION_ID';
my %query;

for my $index ( 1 .. scalar(@dbname_cases) )
{
    my $tbnm = $dbname_cases[$index - 1];
    $query{'insert_' . $tbnm} = $dbi->prepare( "insert ignore into `account_$tbnm` ( `data` ) values ( ? )" );
    $query{'select_' . $tbnm} = $dbi->prepare( "select id from `account_$tbnm` where data=?" );
}

{ # create update statement for main account list
$query{'insert_account_list'} = $dbi->prepare( "insert ignore into `account_list` ( `$account_key` ) values ( ? )" );

    my $tmp = 'update `account_list` set ';

    for my $case ( sort keys %entry_fields )
    {
        $tmp .= "`$case`=? ";
    }

    $tmp =~ s/\? `/?, `/g;
    $tmp .= "where `$account_key`=?";

    $query{'update_account_list'} = $dbi->prepare( $tmp );
}

{ # create update statement for address list
$query{'insert_address_loc'} = $dbi->prepare( "insert ignore into `account_ats_id` ( `$atsid_key` ) values ( ? )" );

    my $tmp = 'update `account_ats_id` set ';

    for my $case ( sort keys %atsid_fields )
    {
        $tmp .= "`$case`=? ";
    }

    $tmp =~ s/\? `/?, `/g;
    $tmp .= "where `$atsid_key`=?";
#print STDERR "$tmp\n";
    $query{'update_address_loc'} = $dbi->prepare( $tmp );
}


#system( "cp ../brecsrvdc/Mapping\\ Installation/Services.csv /tmp/Services.csv" );
system( q# /usr/bin/psql -h warehouse.brec.local 'Black River' viewer -c '\copy (select * from cis.vw_services) to /tmp/Services.csv with csv header' # );
$csv->open_file("/tmp/Services.csv");
$csv->read_header();

$debug = 0;
$count = 0;

while( my %line = $csv->fetchrow_hash )
{
    my @data;

    for my $key ( keys %line )
    {
        $line{$key} = '' unless( defined($line{$key}) );
#        delete $line{$key} unless( defined($line{$key}) );
    }

    for my $index ( 1 .. scalar(@insert_cases) )
    {
        my $case = $insert_cases[$index - 1];
        my $tbnm = $dbname_cases[$index - 1];
#        print sprintf( "ent\tcase: [%s]\tfield: [%s]\tline: [%s]\n", $case, $entry_fields{$case}, $line{$entry_fields{$case}} )
#            if( defined($entry_fields{$case}) and defined($line{$entry_fields{$case}}) );
#        print sprintf( "ats\tcase: [%s]\tfield: [%s]\tline: [%s]\n", $case, $atsid_fields{$case}, $line{$atsid_fields{$case}} )
#            if( defined($atsid_fields{$case}) and defined($line{$atsid_fields{$case}}) );
#print $atsid_fields{$case} . "\n";
#print Dumper( %line );
        if( (defined($entry_fields{$case})) and defined($line{$entry_fields{$case}}) and ($line{$entry_fields{$case}} ne '') )
        {
            $query{'insert_' . $tbnm}->execute( $line{$entry_fields{$case}} );
            $query{'select_' . $tbnm}->execute( $line{$entry_fields{$case}} );

            my $ret=$query{'select_' . $tbnm}->fetchrow_hashref;

            $line{$entry_fields{$case}} = $ret->{'id'};
        } elsif( (defined($atsid_fields{$case})) and defined($line{$atsid_fields{$case}}) 
                and ($line{$atsid_fields{$case}} ne '') ) {
            $query{'insert_' . $tbnm}->execute( $line{$atsid_fields{$case}} );
            $query{'select_' . $tbnm}->execute( $line{$atsid_fields{$case}} );

            my $ret=$query{'select_' . $tbnm}->fetchrow_hashref;

            $line{$atsid_fields{$case}} = $ret->{'id'};
        } else {
            $line{$entry_fields{$case}} = 0 if(defined($entry_fields{$case}));
            $line{$atsid_fields{$case}} = 0 if(defined($atsid_fields{$case}));
        }
    }

    { # scrubbing zip code
        if( defined($line{$atsid_fields{'zip'}}) and $line{$atsid_fields{'zip'}} =~ m/(.....)(....)/ )
        {
            $line{$atsid_fields{'zip'}} = $1;
            $line{$atsid_fields{'zip_4'}} = $2;
        }
    }

    @data = ();
    for my $case ( sort keys %entry_fields )
    {
        $line{$entry_fields{$case}} = '' unless( defined($line{$entry_fields{$case}}) );
        push @data, $line{$entry_fields{$case}};
    }

    $query{'insert_account_list'}->execute( $line{$account_record} );
    $query{'update_account_list'}->execute( @data, $line{$account_record} );

    @data = ();
    for my $case ( sort keys %atsid_fields )
    {
        $line{$atsid_fields{$case}} = '' unless( defined($line{$atsid_fields{$case}}) );
        push @data, $line{$atsid_fields{$case}};
    }

    $query{'insert_address_loc'}->execute( $line{$atsid_record} );
    $query{'update_address_loc'}->execute( @data, $line{$atsid_record} );

    $count++;

    if( $debug )
    {
        if( ($count % 5) == 0 )
        {
            print STDERR "_$count";
        }
        if( ($count % 100) == 0 )
        {
            print STDERR "\n";
        }
        if( ($count % 1000) == 0 )
        {
            last;
        }
    }
}


#system( "cp ../brecsrvdc/Mapping\\ Installation/Meters.csv /tmp/Meters.csv" );
system( q# /usr/bin/psql -h warehouse.brec.local 'Black River' viewer -c '\copy (select * from cis.vw_meter) to /tmp/Meters.csv with csv header' # );
$csv2->open_file("/tmp/Meters.csv");
$csv2->read_header();

$debug = 0;
$count = 0;

while( my %line = $csv2->fetchrow_hash )
{
    if( defined($line{"METER_BOOK"}) and defined($line{"BOOK_SEQUENCE"}) and defined($line{"METERNO"}) )
    {
        $dbi->do( "UPDATE IGNORE `account_list` SET `route_book`='" . $line{"METER_BOOK"} . "', " .
              " `route_seq`='" . $line{"BOOK_SEQUENCE"} . "' WHERE `meterno`='" .
              $line{"METERNO"} . "'" );
    }
}

unlink( "/tmp/Services.csv" );
unlink( "/tmp/Meters.csv" );
