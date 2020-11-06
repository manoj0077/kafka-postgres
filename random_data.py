from faker import Faker

""" Uses faker module to generate persons random data """


def random_data(count=10):
    """
    Generates random data(name, job and phone_number) for count number of persons.

    Args:
        count: Number of persons data

    Returns:
         List of dictionary objects containing person data
    """

    faker = Faker()
    persons_data = []
    for i in range(count):
        person = {"name": faker.name(), "job": faker.job(), "phone_number": faker.phone_number()}
        persons_data.append(person)

    return persons_data


