python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDNBEAMS 36" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDBANDOFFSET 0" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDNBANDS 33" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDFREQUENCY 1340.5" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDMODE search2beamlow" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CMDWRITEFIL 0" -t 20
python /mpikat/mpikat/core/scpi_client.py -H 134.104.64.51 -p 5004 -m "PAFBE:CONFIGURE" -t 200
