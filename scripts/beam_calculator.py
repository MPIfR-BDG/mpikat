import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('config')

def next_power_of_two(n):
    """
    @brief  Round a number up to the next power of two
    """
    return 2**(n-1).bit_length()

def processing_limit_max_bandwidth(nantennas):
    # This is basically educated guesswork at the moment
    # the beanfarmer benchmarking suggests these are reasonable
    # expectations, but reality may be more conservative
    BANDWIDTH = 856e6 # MHz
    nantennas = next_power_of_two(nantennas)
    nbeams = {
        4:3000,
        8:2500,
        16:2000,
        32:1500,
        64:960,
    }
    return nbeams[nantennas]

def _sanitize_sb_configuration(nantennas, tscrunch, fscrunch, desired_nbeams, beam_granularity=1):
    # Constants for data rates and bandwidths
    # these are hardcoded here for the moment but ultimately
    # they should be moved to a higher level or even dynamically
    # specified
    n_servers = next_power_of_two(nantennas)
    n_mcast_groups = 128
    MAX_RATE_PER_MCAST = 6.8e9 # bits/s
    MAX_RATE_PER_SERVER = 4.375e9 # bits/s, equivalent to 280 Gb/s over 64 (virtual) nodes
    BANDWIDTH = 856e6 # MHz
    MINIMUM_NUM_MCAST_GROUPS = 16
    # Calculate the data rate for each beam assuming 8-bit packing and
    # no metadata overheads
    data_rate_per_beam = BANDWIDTH / tscrunch / fscrunch * 8 # bits/s
    log.debug("Data rate per coherent beam: {} Gb/s".format(data_rate_per_beam/1e9))
    # Calculate the maximum number of beams that will fit in one multicast
    # group assuming. Each multicast group must be receivable on a 10 GbE
    # connection so the max rate must be < 8 Gb/s
    max_beams_per_mcast = MAX_RATE_PER_MCAST // data_rate_per_beam
    log.debug("Maximum number of beams per multicast group: {}".format(int(max_beams_per_mcast)))
    if max_beams_per_mcast == 0:
        raise Exception("Data rate per beam is greater than the data rate per multicast group")
    # For instuments such as TUSE, they require a fixed number of beams per node. For their
    # case we assume that they will only acquire one multicast group per node and as such
    # the minimum number of beams per multicast group should be whatever TUSE requires.
    # Multicast groups can contain more beams than this but only in integer multiples of
    # the minimum
    if beam_granularity > 1:
        if max_beams_per_mcast < beam_granularity:
            log.warning("Cannot fit {} beams into one multicast group, updating number of beams per multicast group to {}".format(
                beam_granularity, max_beams_per_mcast))
            beam_granularity = max_beams_per_mcast
        beams_per_mcast = beam_granularity * (max_beams_per_mcast // beam_granularity)
        log.debug("Number of beams per multicast group, accounting for granularity: {}".format(int(beams_per_mcast)))
    else:
        beams_per_mcast = max_beams_per_mcast
    # Calculate the total number of beams that could be produced assuming the only
    # rate limit was that limit per multicast groups
    max_beams = n_mcast_groups * beams_per_mcast
    log.debug("Maximum possible beams (assuming on multicast group rate limit): {}".format(max_beams))
    if desired_nbeams > max_beams:
        log.warning("Requested number of beams is greater than theoretical maximum, "
            "updating setting the number of beams of beams to {}".format(max_beams))
        desired_nbeams = max_beams
    # Calculate the total number of multicast groups that are required to satisfy
    # the requested number of beams
    num_mcast_groups_required = round(desired_nbeams / beams_per_mcast + 0.5)
    log.debug("Number of multicast groups required for {} beams: {}".format(desired_nbeams, num_mcast_groups_required))
    actual_nbeams = num_mcast_groups_required * beams_per_mcast
    nmcast_groups = num_mcast_groups_required
    # Now we need to check the server rate limits
    if (actual_nbeams * data_rate_per_beam)/n_servers > MAX_RATE_PER_SERVER:
        log.warning("Number of beams limited by output data rate per server")
    actual_nbeams = MAX_RATE_PER_SERVER* n_servers // data_rate_per_beam

    if nmcast_groups < MINIMUM_NUM_MCAST_GROUPS:
        # The traffic rate is low here so maybe this isn't so much of a problem
        # need to ask Jason
        log.warning("The beams should occupy at least {} multicast groups".format(MINIMUM_NUM_MCAST_GROUPS))

    # Now we check against what can actually be done in the GPUs
    max_processable_beams = processing_limit_max_bandwidth(nantennas)
    if max_processable_beams < actual_nbeams:
        log.warning("Maximum number of beams is limited by processing power to {}".format(max_processable_beams))
        actual_nbeams = max_processable_beams
    log.info("Number of beams that can be generated: {}".format(actual_nbeams))
    return actual_nbeams