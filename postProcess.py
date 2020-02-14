#!/usr/bin/env python
import sys
import argparse
import tarfile
import os
from io import BytesIO


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Build tar ball from observation data and fix time stamp on the fly')
    parser.add_argument('folder', type=str, 
                    help='files to accumulate')

    args = parser.parse_args()

    ofname = args.folder.rstrip('/') + '.tar'

    print("Writing output to {}".format(ofname))
    tar = tarfile.open(ofname, 'w')
    for f in os.listdir(args.folder):
        fname = os.path.join(args.folder, f)
        print("Reading {}".format(fname))
        data = open(fname, 'rb').read()
        #print(data[:4096])\



        timestampStart = data[:4096].find("UTC_START")
        timestampStop = min(data[timestampStart:4096].find("\n"),data[timestampStart:4096].find("#")) 
        lH = data[timestampStart:timestampStop+timestampStart].rfind('-')

        oData = bytearray(data)
        oData[timestampStart +lH] = 'T'

        of = BytesIO(oData)


        info = tarfile.TarInfo(name=f)
        info.size = len(oData)

        tar.addfile(tarinfo=info, fileobj=of)
    
    tar.close()

