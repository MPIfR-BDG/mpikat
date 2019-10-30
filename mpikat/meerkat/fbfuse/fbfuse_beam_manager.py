"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

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
import StringIO
import mosaic
from katpoint import Target

log = logging.getLogger("mpikat.fbfuse_ca_server")

DEFAULT_KATPOINT_TARGET = "unset, radec, 0, 0"


class BeamAllocationError(Exception):
    pass


class Beam(object):
    """Wrapper class for a single beam to be produced
    by FBFUSE"""
    def __init__(self, idx, target=DEFAULT_KATPOINT_TARGET):
        """
        @brief   Create a new Beam object

        @params   idx   a unique identifier for this beam.

        @param      target          A KATPOINT target object
        """
        self.idx = idx
        self._target = target
        self._observers = set()

    @property
    def target(self):
        return self._target

    @target.setter
    def target(self, new_target):
        self._target = new_target
        self.notify()

    def notify(self):
        """
        @brief  Notify all observers of a change to the beam parameters
        """
        for observer in self._observers:
            observer(self)

    def register_observer(self, func):
        """
        @brief   Register an observer to be called on a notify

        @params  func  Any function that takes a Beam object as its only argument
        """
        self._observers.add(func)

    def deregister_observer(self, func):
        """
        @brief   Deregister an observer to be called on a notify

        @params  func  Any function that takes a Beam object as its only argument
        """
        self._observers.remove(func)

    def reset(self):
        """
        @brief   Reset the beam to default parameters
        """
        self.target = Target(DEFAULT_KATPOINT_TARGET)

    def __repr__(self):
        return "{}, {}".format(
            self.idx, self.target.format_katcp())


class Tiling(object):
    """Wrapper class for a collection of beams in a tiling pattern
    """
    def __init__(self, target, antennas, reference_frequency, overlap):
        """
        @brief   Create a new tiling object

        @param      target          A KATPOINT target object

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]
        """
        self._beams = []
        self.target = target
        self._antennas = antennas
        self.reference_frequency = reference_frequency
        self.overlap = overlap
        self.tiling = None

    @property
    def nbeams(self):
        return len(self._beams)

    def add_beam(self, beam):
        """
        @brief   Add a beam to the tiling pattern

        @param   beam   A Beam object
        """
        self._beams.append(beam)

    def generate(self, epoch):
        """
        @brief   Calculate and update RA and Dec positions of all
                 beams in the tiling object.

        @param      epoch     The epoch of tiling (unix time)

        @param      antennas  The antennas to use when calculating the beam shape.
                              Note these are the antennas in katpoint CSV format.
        """
        log.debug("Creating PSF simulator at reference frequency {} Hz".format(
            self.reference_frequency))
        psfsim = mosaic.PsfSim(self._antennas, self.reference_frequency)
        log.debug(("Generating beam shape for target position {} "
                   "at epoch {}").format(self.target, epoch))
        beam_shape = psfsim.get_beam_shape(self.target, epoch)
        log.debug("Generating tiling of {} beams with an overlap of {}".format(
            self.nbeams, self.overlap))
        tiling = mosaic.generate_nbeams_tiling(
            beam_shape, self.nbeams, self.overlap)
        coordinates = tiling.get_equatorial_coordinates()
        for ii in range(min(tiling.beam_num, self.nbeams)):
            ra, dec = coordinates[ii]
            self._beams[ii].target = Target('{},radec,{},{}'.format(
                self.target.name, ra, dec))
        return tiling

    def __repr__(self):
        return ", ".join([repr(beam) for beam in self._beams])

    def idxs(self):
        return ",".join([beam.idx for beam in self._beams])


class BeamManager(object):
    """Manager class for allocation, deallocation and tracking of
    individual beams and static tilings.
    """
    def __init__(self, nbeams, antennas):
        """
        @brief  Create a new beam manager object

        @param  nbeams    The number of beams managed by this object

        @param  antennas  A list of antennas to use for tilings. Note these should
                          be in KATPOINT CSV format.
        """
        self._nbeams = nbeams
        self._antennas = antennas
        self._beams = [Beam("cfbf%05d"%(i)) for i in range(self._nbeams)]
        self._free_beams = [beam for beam in self._beams]
        self._allocated_beams = []
        self.reset()

    @property
    def nbeams(self):
        return self._nbeams

    @property
    def antennas(self):
        return self._antennas

    def generate_psf_png(self, target, reference_frequency, epoch):
        psfsim = mosaic.PsfSim(self._antennas, reference_frequency)
        beam_shape = psfsim.get_beam_shape(target, epoch)
        png = StringIO.StringIO()
        beam_shape.plot_psf(png, shape_overlay=True)
        png.seek(0)
        return png.read()

    def reset(self):
        """
        @brief  reset and deallocate all beams and tilings managed by this instance

        @note   All tiling will be lost on this call and must be remade for subsequent observations
        """
        for beam in self._beams:
            beam.reset()
        self._free_beams = [beam for beam in self._beams]
        self._allocated_beams = []
        self._tilings = []

    def add_beam(self, target):
        """
        @brief   Specify the parameters of one managed beam

        @param      target          A KATPOINT target object

        @return     Returns the allocated Beam object
        """
        try:
            beam = self._free_beams.pop(0)
        except IndexError:
            raise BeamAllocationError("No free beams remaining")
        beam.target = target
        self._allocated_beams.append(beam)
        return beam

    def add_tiling(self, target, nbeams, reference_frequency, overlap):
        """
        @brief   Add a tiling to be managed

        @param      target          A KATPOINT target object

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]

        @note       This function will not raise an exception in the event that the user over-requests beams
                    it is incumbent upon the end user to check what has actually been allocated in the tiling.

        @returns    The created Tiling object
        """
        tiling = Tiling(target, self._antennas, reference_frequency, overlap)
        for _ in range(nbeams):
            if len(self._free_beams) == 0:
                log.warning("Unable to allocate all beams in tiling: {} of {} allocated".format(
                    tiling.nbeams, nbeams))
                break
            else:
                beam = self._free_beams.pop(0)
                tiling.add_beam(beam)
                self._allocated_beams.append(beam)
        self._tilings.append(tiling)
        return tiling

    def get_beams(self):
        """
        @brief  Return all managed beams
        """
        return self._allocated_beams + self._free_beams

