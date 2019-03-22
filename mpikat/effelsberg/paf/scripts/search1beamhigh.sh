python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDNBEAMS 18" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDBANDOFFSET 0" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDNBANDS 48" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDFREQUENCY 1340.5" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDMODE search1beamhigh" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDZOOMBAND0 33" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDZOOMNBANDS 5" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDWRITEFIL 1" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CONFIGURE" -t 200
