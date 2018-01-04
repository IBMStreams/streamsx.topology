use strict;
use warnings;
use IO::File;

#
# Return true if optional data types are supported, else false.
#
sub hasOptionalTypesSupport {
    # TODO: modify version number to match optional data types support
    return hasMinimumProductVersion("4.2.5");
}

#
# Return true if the Streams product version matches or exceeds
# the given version number in "VRMF" format, else false.
#
# Note: This test assumes the fixpack ("F") is numeric, or not specified.
#
sub hasMinimumProductVersion {
    my ($requiredVersion) = @_;

    my $productVersion = "";
    for (`$ENV{'STREAMS_INSTALL'}/bin/streamtool version`) {
        $productVersion = $_ if s/Version=//;
    }

    my @pvrmf = split(/\./, $productVersion);
    my @vrmf = split(/\./, $requiredVersion);
    for (my $i = 0; $i <= $#vrmf; $i++) {
        if (!($vrmf[$i] =~ /^\d+$/)) {
            print STDERR "ERROR: Invalid version: $requiredVersion\n";
            return 0;
        }
        return 0 if ($i > $#pvrmf);
        return 0 if ($pvrmf[$i] < $vrmf[$i]);
        return 1 if ($pvrmf[$i] > $vrmf[$i]);
    }
    return 1;
}

1;
