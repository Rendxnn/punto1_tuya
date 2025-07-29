import pandas as pd
from faker import Faker
import random
import os

def generate_dummy_data(num_records: int, output_path: str):
    fake = Faker('es_ES')  
    data = []

    for i in range(num_records):
        customer_id = i + 1
        customer_name = fake.name()
        email = fake.email()

        if i % 10 == 0:  # 10% invalid format
            phone_number = ''.join(random.choices('abcdefg', k=random.randint(5, 15))) # Random letters
        elif i % 7 == 0: # 14% with wrong length
            phone_number = fake.numerify(text='##########') # 10 digits
            if random.random() < 0.5:
                phone_number = phone_number[:random.randint(1, 9)] # Too short
            else:
                phone_number = phone_number + fake.numerify(text='###') # Too long
        elif i % 5 == 0: # 20% duplicates
            phone_number = '+34600123456' # A fixed duplicate number
        else:
            phone_number = fake.phone_number()

        # Introduce some nulls/empty strings
        if i % 20 == 0:
            email = None
        if i % 25 == 0:
            customer_name = ''

        data.append({
            'customer_id': customer_id,
            'customer_name': customer_name,
            'email': email,
            'phone_number': phone_number
        })

    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Generated {num_records} records to {output_path}")

if __name__ == "__main__":
    num_records = 1_000_000
    output_dir = r"C:\Users\samir\Documents\freelance\entrevista_tuya\punto_1\data"
    output_file = os.path.join(output_dir, "dummy_phone_numbers.csv")

    os.makedirs(output_dir, exist_ok=True)
    generate_dummy_data(num_records, output_file)
