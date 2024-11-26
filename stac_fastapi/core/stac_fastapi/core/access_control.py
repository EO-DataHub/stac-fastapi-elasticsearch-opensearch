import hashlib
import os

# Set the size of the hash table
NUMBER_OF_USERS = os.getenv("NUMBER_OF_USERS", 4096)  # Default to 1024 users


def hash_to_index(
    value, bitstring_size=NUMBER_OF_USERS
):  # 1023 in binary is 1111111111
    if not value:
        # If no user provided, return public index
        return NUMBER_OF_USERS - 1
    # Compute SHA-256 hash and take the last X bits for the index
    hash_object = hashlib.sha256(value.encode())
    hash_digest = hash_object.digest()
    hash_int = int.from_bytes(hash_digest, byteorder="big")
    index = hash_int & (bitstring_size - 2)  # Exclude the fully-public bit
    return index


def set_bit(bitstring, index, allow=True):
    if allow:
        bitstring[index] = 1
    else:
        bitstring[index] = 0


def create_bitstring(ids=[], is_public=False) -> str:
    bitstring_size = NUMBER_OF_USERS
    bitstring = [0] * bitstring_size
    # Handle user access bits
    for id in ids:
        index = hash_to_index(id)
        set_bit(bitstring, index)
    # Set the fully-public bit
    if is_public:
        bitstring[-1] = 1
    bitstring = "".join(str(bit) for bit in bitstring)
    return bitstring


# Not used
def add_user(bitstring, new_user):
    index = hash_to_index(new_user, len(bitstring))
    set_bit(bitstring, index)
    return bitstring


# Not used
def remove_user(bitstring, user):
    index = hash_to_index(user, len(bitstring))
    set_bit(bitstring, index, allow=False)
    return bitstring
