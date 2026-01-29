"""
Process Status Decoder

Decodes bitwise status fields from equipment controllers into
human-readable process states. Supports both current state
and complete state history decoding.
"""


def decode_status_current(status_value):
    """
    Decodes the status_field value to get the current process state
    
    Finds the highest active bit to determine the current state.
    
    Args:
        status_value: Numeric value representing the 6 status bits
        
    Returns:
        str: Description of the current process state
    """
    STATUS_BITS = {
        1: 'start_phase_1',
        2: 'start_phase_2', 
        3: 'start_phase_3',
        4: 'start_phase_4',
        5: 'start_phase_5',
        6: 'start_phase_6',
    }
    
    if status_value is None or status_value == 0:
        return 'no_status'
    
    try:
        status_int = int(status_value)
    except (ValueError, TypeError):
        return 'invalid_value'
    
    # Find the highest active bit (current state)
    for bit_position in range(6, 0, -1):  # 6 down to 1
        if status_int & (1 << (bit_position - 1)):
            return STATUS_BITS.get(bit_position, f'unknown_status_bit_{bit_position}')
    
    return 'no_status'


def decode_status_complete(status_value):
    """
    Decodes the status_field value to get ALL executed states
    
    Returns a space-separated list of all states that have been
    executed in chronological order.
    
    Args:
        status_value: Numeric value representing the 6 status bits
        
    Returns:
        str: Space-separated list of all executed states
    """
    STATUS_BITS = {
        1: 'start_phase_1',
        2: 'start_phase_2', 
        3: 'start_phase_3',
        4: 'start_phase_4',
        5: 'start_phase_5',
        6: 'start_phase_6',
    }
    
    if status_value is None or status_value == 0:
        return 'no_states'
    
    try:
        status_int = int(status_value)
    except (ValueError, TypeError):
        return 'invalid_value'
    
    # Find all active bits (complete state history)
    active_states = []
    for bit_position in range(1, 7):  # 1 to 6 (chronological)
        if status_int & (1 << (bit_position - 1)):
            state_desc = STATUS_BITS.get(bit_position, f'unknown_status_bit_{bit_position}')
            active_states.append(state_desc)
    
    if not active_states:
        return 'no_states'
    
    return ' '.join(active_states)


def test_decode_status():
    """Test function to verify the decoder"""
    test_values = [0, 1, 2, 3, 4, 7, 15, 31, 63]
    
    print("Status field decoder tests:")
    print("-" * 90)
    print(f"{'Value':<7} | {'Binary':<8} | {'Current Status':<20} | {'Complete History'}")
    print("-" * 90)
    
    for value in test_values:
        current = decode_status_current(value)
        complete = decode_status_complete(value)
        binary = format(value, '06b') if value is not None else 'N/A'
        print(f"{value:2d}      | {binary:<8} | {current:<20} | {complete}")


if __name__ == "__main__":
    test_decode_status()