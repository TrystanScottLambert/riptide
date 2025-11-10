"""
Filter checking module to handle searching for erroneous filters that might exist.
"""

valid_filters = [
    "FUV_GALEX",
    "NUV_GALEX",
    "u_SDSS",
    "g_SDSS",
    "r_SDSS",
    "i_SDSS",
    "z_SDSS",
    "u_VST",
    "g_VST",
    "r_VST",
    "i_VST",
    "Z_VISTA",
    "Y_VISTA",
    "J_VISTA",
    "H_VISTA",
    "K_VISTA",
    "W1_WISE",
    "I1_Spitzer",
    "I2_Spitzer",
    "W2_WISE",
    "I3_Spitzer",
    "I4_Spitzer",
    "W3_WISE",
    "W4_WISE",
    "M24_Spitzer",
    "M70_Spitzer",
    "P70_Herschel",
    "P100_Herschel",
    "P160_Herschel",
    "S250_Herschel",
    "S350_Herschel",
    "S450_JCMT",
    "S500_Herschel",
    "S850_JCMT",
    "Band_ionising_photons",
    "Band9_ALMA",
    "Band8_ALMA",
    "Band7_ALMA",
    "Band6_ALMA",
    "Band5_ALMA",
    "Band4_ALMA",
    "Band3_ALMA",
    "BandX_VLA",
    "BandC_VLA",
    "BandS_VLA",
    "BandL_VLA",
    "Band_610MHz",
    "Band_325MHz",
    "Band_150MHz",
]


def validate_filter_name(input_string):
    """
    Check if a string is attempting to use a filter name incorrectly.

    Args:
        input_string: The string to validate
        valid_filters: List of valid filter names (e.g., ['FUV_GALEX', 'NUV_GALEX'])

    Returns:
        tuple: (is_valid, recommended_filter)
            - is_valid: True if exact match or no violation, False if violation detected
            - recommended_filter: The correct filter name if violation, None otherwise
    """

    # Check for exact match first
    if input_string in valid_filters:
        return (True, None)

    # Normalize for comparison (remove case, underscores, hyphens)
    def normalize(s):
        return s.lower().replace("_", "").replace("-", "")

    # Remove common prefixes from input
    input_lower = input_string.lower()
    stripped_input = input_string
    for prefix in ["filter", "filt", "band"]:
        if input_lower.startswith(prefix):
            # Check if what follows looks like it could be a filter
            remainder = input_string[len(prefix) :]
            if remainder and remainder[0].isupper():
                # Only strip if next char is uppercase (e.g., filterFUV, bandW1)
                stripped_input = remainder
                input_lower = stripped_input.lower()
                break

    normalized_input = normalize(stripped_input)

    # Check each valid filter
    for valid_filter in valid_filters:
        normalized_filter = normalize(valid_filter)

        # Direct normalized match (handles case and underscores)
        if normalized_input == normalized_filter:
            return (False, valid_filter)

        # Check if the input contains the filter parts in any order
        # Split on underscores to get parts
        filter_parts = valid_filter.lower().split("_")

        # Check if all parts appear in the stripped input (in any order, any case)
        all_parts_present = all(part in input_lower for part in filter_parts)

        if all_parts_present and len(filter_parts) > 1:
            # Don't flag if the string is much longer than the filter (e.g., FUV_GALEX_test)
            # This indicates it's not trying to BE the filter, just contains it
            length_ratio = len(normalized_input) / len(normalized_filter)
            if length_ratio > 1.5:
                continue

            # Calculate a match score based on character overlap
            # This helps distinguish between similar filters
            overlap = sum(c in normalized_input for c in normalized_filter)
            match_ratio = overlap / max(len(normalized_input), len(normalized_filter))

            # If substantial overlap and all parts present, it's likely this filter
            if match_ratio > 0.65:
                return (False, valid_filter)

    # No violation detected - either valid or doesn't match any filter
    return (True, None)


# Example usage
if __name__ == "__main__":

    test_cases = [
        "FUV_GALEX",  # Valid - exact match
        "fuvGALEX",  # Invalid - wrong case, missing underscore
        "filterFUVgalex",  # Invalid - prefix + wrong case
        "FuvGalex",  # Invalid - camelCase
        "GALEXfuv",  # Invalid - reordered
        "galex_fuv",  # Invalid - wrong case, reordered
        "fuv_galex",  # Invalid - wrong case
        "filtW1WISE",  # Invalid - prefix + wrong format
        "bandW1Wise",  # Invalid - prefix + camelCase
        "FUV_GALEX_test",  # Valid - doesn't match any filter
        "my_custom_filter",  # Valid - doesn't match any filter
        "u_SDSS",  # Valid - exact match
        "uSDSS",  # Invalid - missing underscore
        "SDSS_u",  # Invalid - reordered
        "Band7ALMA",  # Invalid - missing underscore
        "filterBand7Alma",  # Invalid - prefix + wrong case
        "mag_err_VISTA_Z",  # Invalid - should be Z_VISTA
        "VISTA_Z",  # Invalid - reordered
        "flux_Z_VISTA",  # Valid - correct filter in compound name
    ]

    print("Filter Validation Results:")
    print("-" * 80)
    for test in test_cases:
        is_valid, recommended = validate_filter_name(test)
        status = "✓ VALID" if is_valid else "✗ VIOLATION"
        correction = f" → Use: {recommended}" if recommended else ""
        print(f"{status:12} | '{test:25}'{correction}")

    # Test snake_case validation
    print("\n" + "=" * 80)
    print("Snake Case Validation Results:")
    print("-" * 80)

    snake_case_tests = [
        "my_variable_name",  # Valid snake_case
        "filter_data_v2",  # Valid snake_case
        "myVariableName",  # Invalid - camelCase
        "MyVariableName",  # Invalid - PascalCase
        "my-variable-name",  # Invalid - kebab-case
        "_leading_underscore",  # Invalid - leading underscore
        "trailing_underscore_",  # Invalid - trailing underscore
        "double__underscore",  # Invalid - consecutive underscores
        "UPPERCASE_NAME",  # Invalid - uppercase
        "mix_Case_name",  # Invalid - mixed case
        "number123_test",  # Valid - contains numbers
        "123number",  # Valid - starts with number
        "just_underscores_",  # Invalid - trailing underscore
        "simple",  # Valid - single word lowercase
    ]
