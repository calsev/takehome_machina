def array_of_struct_to_struct_of_array(array_of_structs: list[dict]) -> dict[str, list]:
    """
    Converts an array of dicts to a dict of arrays with the same keys
    :param array_of_structs: List of dicts, all with the same keys
    :return: Dict of lists
    """
    struct_of_arrays = {key: [] for key in array_of_structs[0]}
    for struct in array_of_structs:
        for key in struct_of_arrays:
            struct_of_arrays[key].append(struct[key])
    return struct_of_arrays
