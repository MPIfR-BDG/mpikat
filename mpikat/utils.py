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
from katcp import Sensor

class AntennaValidationError(Exception):
    pass

def is_power_of_two(n):
    """
    @brief  Test if number is a power of two

    @return True|False
    """
    return n != 0 and ((n & (n - 1)) == 0)

def next_power_of_two(n):
    """
    @brief  Round a number up to the next power of two
    """
    return 2**(n-1).bit_length()

def parse_csv_antennas(antennas_csv):
    antennas = antennas_csv.split(",")
    nantennas = len(antennas)
    if nantennas == 1 and antennas[0] == '':
        raise AntennaValidationError("Provided antenna list was empty")
    names = [antenna.strip() for antenna in antennas]
    if len(names) != len(set(names)):
        raise AntennaValidationError("Not all provided antennas were unqiue")
    return names

class LoggingSensor(Sensor):
    def __init__(self, *args, **kwargs):
        self.logger = None
        super(LoggingSensor, self).__init__(*args, **kwargs)

    def set_value(self, value):
        if self.logger:
            self.logger.debug("Sensor '{}' changed from '{}' to '{}'".format(
                self.name, self.value(), value))
        super(LoggingSensor, self).set_value(value)

    def set_logger(self, logger):
        self.logger = logger