"""This module demonstrates a simple addition operation using a helper function."""

# Define a function named 'add' that takes two arguments, 'number1' and 'number2'.
# The purpose of this function is to add the two numbers and return the result.
def add(number1, number2):
    """Return the sum of number1 and number2."""
    # Return the sum of 'number1' and 'number2'.
    # This line computes the addition of the two input numbers and outputs the result.
    return number1 + number2

# Initialize the constant variable 'NUM1' with the value 4.
# Constants are usually written in uppercase letters to indicate that they should not be changed.
NUM1 = 4

# Initialize the constant variable 'NUM2' with the value 5.
# This variable will be used as the second input to the 'add' function.
NUM2 = 5

# Call the 'add' function with 'NUM1' and 'NUM2' as arguments.
# The result of this addition operation is stored in the variable 'TOTAL'.
TOTAL = add(NUM1, NUM2)

# Print a formatted string that displays the sum of 'NUM1' and 'NUM2'.
# An f-string is used to insert the values of 'NUM1', 'NUM2', and 'TOTAL' into the string.
print(f"The sum of {NUM1} and {NUM2} is {TOTAL}")
