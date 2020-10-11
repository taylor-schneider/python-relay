import numpy

def convert_date_string_to_date(input_string):

    # We then do our manipulation
    input_string = input_string.strip()

    # Make it a date
    result = numpy.datetime64(input_string, 'D')

    return result