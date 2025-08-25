# my_module.pyx
cimport numpy as cnp
import numpy as np
cimport cython
from cython cimport view
from cython.view cimport array as cvarray
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from libc.stdint cimport int64_t, uint64_t
from libc.stdio cimport *

cdef extern from "stdio.h":
    FILE *fopen(const char *, const char *)
    int fclose(FILE *)
    ssize_t getline(char **, size_t *, FILE *)
    ssize_t fread(void* ptr, size_t size, size_t count, FILE* stream)
    int feof(FILE* stream)


# cdef uint64_t max_value = 0x7e7e7e7e7e7e7e7e
# cdef uint64_t min_value = 0x2020202020202020
cdef int MIN_CHAR_ASCII = 32  # ' '
cdef int MAX_CHAR_ASCII = 126 # '~'
cdef int range_per_char = MAX_CHAR_ASCII - MIN_CHAR_ASCII
cdef int base = range_per_char + 1


cdef int get_partition(const char* line, int num_partitions):

    cdef int i
    cdef unsigned long long numerical_value = 0
    cdef unsigned long long max_numerical_value = 0
    cdef int normalized_char_val
    cdef int partition
    cdef double normalized_value

    for i in range(8):
        normalized_char_val = line[i] - MIN_CHAR_ASCII
        numerical_value = numerical_value * base + normalized_char_val

    for i in range(8):
        max_numerical_value = max_numerical_value * base + range_per_char

    # --- Map the numerical value to a partition ---
    # Avoid division by zero if the range is empty.
    if max_numerical_value == 0:
        return 0

    normalized_value = <double>numerical_value / <double>max_numerical_value
    partition = <int>(normalized_value * num_partitions)

    if partition >= num_partitions:
        partition = num_partitions - 1

    return partition


@cython.boundscheck(False)
@cython.wraparound(False)
def read_terasort_data(data, num_partitions):

    cdef int64_t data_len = len(data)

    cdef list key_list = []
    cdef list value_list = []
    cdef list partition_list = []
    cdef list true_values = []

    data_byte_string = data
    cdef char * cdata = data_byte_string

    cdef size_t l = 0
    cdef char * line = <char *> malloc(sizeof(char) * 100)

    cdef uint64_t current_pos = 0
    while current_pos < data_len:
        
        memcpy(line, cdata + (current_pos * sizeof(char)), sizeof(char)*100)
        partition = get_partition(line, num_partitions)
        key = line[0:10].decode('utf-8')
        value = line[12:98].decode('utf-8')

        key_list.append(key)
        value_list.append(value)
        partition_list.append(partition)
        
        current_pos += 100


    # fclose(cfile)

    return key_list, value_list, np.array(partition_list, dtype=np.uint16)

@cython.boundscheck(False)
@cython.wraparound(False)
def remap_partitions(cnp.ndarray[cnp.uint16_t, ndim=1] input_array, int original_partitions, int new_partitions):
    """
    Remaps partition assignments in an array based on a reduction
    in the total number of partitions. Input values are assumed to be
    in the range [0, original_partitions - 1].
    The output values will be in the range [0, new_partitions - 1].

    Args:
        input_array: A NumPy array of uint16 integers representing the original partition assignments.
        original_partitions: The original total number of partitions. Must be > 0.
        new_partitions: The new total number of partitions (assumed to be > 0 and
                        less than original_partitions).

    Returns:
        A new NumPy array of uint16 integers with the remapped partition assignments.
    """
    cdef int n = input_array.shape[0]
    # Use np.uint16 for the dtype, which is the Python-level dtype object
    cdef cnp.ndarray[cnp.uint16_t, ndim=1] output_array = np.empty_like(input_array, dtype=np.uint16)
    cdef int i
    # cdef int scaling_factor # This variable is no longer needed

    # Basic assertions for valid inputs (can be expanded)
    if new_partitions <= 0 or original_partitions <= 0:
        raise ValueError("Number of partitions must be positive.")
    if new_partitions >= original_partitions:
        raise ValueError("New number of partitions must be less than original number of partitions.")

    # Iterate through the input array and apply the corrected mapping
    for i in range(n):
        # Ensure input values are within the expected range [0, original_partitions - 1]
        # If input_array[i] can exceed original_partitions-1, you might want to add clamping
        # or error handling here, though the formula would still produce a value.
        # For example:
        # current_input_val = input_array[i]
        # if current_input_val >= original_partitions:
        #     # Option 1: Clamp to max valid original index
        #     current_input_val = original_partitions - 1
        #     # Option 2: Raise an error or handle as an invalid input
        #     # raise ValueError(f"Input value {input_array[i]} is out of range for original_partitions {original_partitions}")

        # Corrected mapping formula:
        # (input_value * new_total) / old_total
        # This ensures that for input_array[i] in [0, original_partitions - 1],
        # the output is correctly scaled to [0, new_partitions - 1].
        # The intermediate product (input_array[i] * new_partitions) should fit
        # in a standard integer type (e.g., C long) given that input_array[i] is uint16
        # and new_partitions is typically not excessively large.
        output_array[i] = (input_array[i] * new_partitions) // original_partitions

    return output_array
