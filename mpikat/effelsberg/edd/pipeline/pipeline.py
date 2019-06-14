"""
Copyright (c) 2019 Jason Wu <jwu@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
log = logging.getLogger("mpikat.effelsberg.edd.pipeline.pipeline")
log.setLevel('DEBUG')

try:
    from GatedSpectrometerPipeline import GatedSpectrometerPipeline
    from VLBIPipeline import VLBIPipeline
    from Mkrecv2Db2Dspsr import Mkrecv2Db2Dspsr
except Exception as E:
    log.error("Error importing pipeline: {}".format(E))

from pipeline_register import PIPELINES

#def main():
#    logging.info("Starting pipeline instance")
#    server = Mkrecv2Db2Dspsr()
#    server.configure()
#    server.start()
#    server.stop()
#    server.deconfigure()
#
#if __name__ == "__main__":
#    main()
