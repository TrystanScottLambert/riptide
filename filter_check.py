"""
Filter checking module to handle searching for erroneous filters that might exist.
"""

from typing import Tuple

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

inverse = ["_".join(filter_name.split("_")[::-1]) for filter_name in valid_filters]


def validate_filter(name: str) -> Tuple[bool, str | None]:
    """
    Checks that the string doesn't contain some attempt at using a filter name and if it does
    actively suggests the correct version.
    """
    simplified_string = name.lower().replace("_", "")
    # check cases are correct.
    for filter_name in valid_filters:
        if filter_name.replace("_", "").lower() in simplified_string:
            if filter_name not in name:
                return (False, filter_name)

    # check inverse cases
    for inverse_filter_name, filter_name in zip(inverse, valid_filters):
        if inverse_filter_name.replace("_", "").lower() in simplified_string:
            return (False, filter_name)

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
        is_valid, recommended = validate_filter(test)
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
