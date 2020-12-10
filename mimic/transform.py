import logging
import os
import random
import re

from .tools import ensure_dir, remove_abs


class PlaceholderMapper:

    def __init__(self, lists_replacements):

        self.placeholder_mapping = {}
        self.lists_replacements = lists_replacements

    def get_mapping(self, placeholder):

        mo = re.match("\[\*\*Age over 90 \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(90, 100))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Age over 90 \*\*\]", placeholder)
        if mo:
            return str(random.randint(90, 100))

        mo = re.match("\[\*\*Apartment Address\(\d+\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["addresses"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Apartment Address\(\d+\) \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["addresses"])

        mo = re.match("\[\*\*Attending Info \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                firstname = random.choice(self.lists_replacements["all_first_names"])
                name = random.choice(self.lists_replacements["last_names"])
                self.placeholder_mapping[mo.group(0)] = "{} {}".format(firstname, name)
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Attending Info \*\*\]", placeholder)
        if mo:
            firstname = random.choice(self.lists_replacements["all_first_names"])
            name = random.choice(self.lists_replacements["last_names"])
            return "{} {}".format(firstname, name)

        mo = re.match("\[\*\*CC Contact Info \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["phone_numbers"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*CC Contact Info \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["phone_numbers"])

        mo = re.match("\[\*\*Clip Number \(Radiology\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 10000))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Clip Number \(Radiology\) \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 10000))

        mo = re.match("\[\*\*Company \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["companies"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Company \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["companies"])

        mo = re.match("\[\*\*Country \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["countries"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Country \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["countries"])

        mo = re.match("\[\*\*Date (r|R)ange (\(\d+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                year_begin, month_begin, day_begin, year_end, month_end, day_end = self._build_date_range()
                self.placeholder_mapping[mo.group(0)] = "{}/{}/{}-{}/{}/{}".format(
                    year_begin,
                    month_begin,
                    day_begin,
                    year_end,
                    month_end,
                    day_end
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Date (r|R)ange (\(\d+\) )?\*\*\]", placeholder)
        if mo:
            year_begin, month_begin, day_begin, year_end, month_end, day_end = self._build_date_range()
            return "{}/{}/{}-{}/{}/{}".format(
                year_begin,
                month_begin,
                day_begin,
                year_end,
                month_end,
                day_end
            )

        mo = re.match("\[\*\*Dictator Info \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 10000))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Dictator Info \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 10000))

        mo = re.match("\[\*\*Doctor First Name \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["all_first_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Doctor First Name \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["all_first_names"])

        mo = re.match("\[\*\*Doctor Last Name (\(ambig\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["last_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Doctor Last Name (\(ambig\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["last_names"])

        mo = re.match("\[\*\*E-mail address \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["emails"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*E-mail address \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["emails"])

        mo = re.match("\[\*\*Female First Name \([^\[]+\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["first_names_female"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Female First Name \([^\[]+\) \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["first_names_female"])

        mo = re.match("\[\*\*First Name(\d+)? (\([^\[]+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["all_first_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*First Name(\d+)? (\([^\[]+\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["all_first_names"])

        mo = re.match("\[\*\*Holiday \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["holidays"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Holiday \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["holidays"])

        mo = re.match("\[\*\*Hospital(\d+)? \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["hospitals"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Hospital(\d+)? \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["hospitals"])

        mo = re.match("\[\*\*Initials? \(NamePattern\d+\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                firstname = random.choice(self.lists_replacements["all_first_names"])
                name = random.choice(self.lists_replacements["last_names"])
                self.placeholder_mapping[mo.group(0)] = "{}{}".format(firstname[0:1], name[0:1])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Initials? \(NamePattern\d+\) \*\*\]", placeholder)
        if mo:
            firstname = random.choice(self.lists_replacements["all_first_names"])
            name = random.choice(self.lists_replacements["last_names"])
            return "{}{}".format(firstname[0:1], name[0:1])

        mo = re.match("\[\*\*Job Number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 10000))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Job Number \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 10000))

        mo = re.match("\[\*\*Known firstname \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["all_first_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Known firstname \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["all_first_names"])

        mo = re.match("\[\*\*Known lastname \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["last_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Known lastname \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["last_names"])

        mo = re.match("\[\*\*Last Name ([^\[]+ )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["last_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Last Name ([^\[]+ )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["last_names"])

        mo = re.match("\[\*\*Location ([^\[]+ )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["locations"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Location ([^\[]+ )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["locations"])

        mo = re.match("\[\*\*MD Number(\(\d+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["phone_numbers"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*MD Number(\(\d+\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["phone_numbers"])

        mo = re.match("\[\*\*Male First Name (\([^[]+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["all_first_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Male First Name (\([^[]+\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["all_first_names"])

        mo = re.match("\[\*\*Medical Record Number (\([^[]+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 10000))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Medical Record Number (\([^[]+\) )?\*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 10000))

        mo = re.match("\[\*\*Month \(only\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["months"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month \(only\) \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["months"])

        mo = re.match("\[\*\*Month Day \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 31))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month Day \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 31))

        mo = re.match("\[\*\*Month/Day (\(?\d+\)? )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month/Day (\(?\d+\)? )?\*\*\]", placeholder)
        if mo:
            return "{}/{}".format(
                str(random.randint(1, 12)),
                str(random.randint(1, 31))
            )

        mo = re.match("\[\*\*Month/Year (\(?\d+\)? )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1950, 2016))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month/Year (\(?\d+\)? )?\*\*\]", placeholder)
        if mo:
            return "{}/{}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1950, 2016))
                )

        mo = re.match("\[\*\*Month/Day/Year \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}/{}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31)),
                    str(random.randint(1950, 2016))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month/Day/Year \*\*\]", placeholder)
        if mo:
            return "{}/{}/{}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31)),
                    str(random.randint(1950, 2016))
                )

        mo = re.match("\[\*\*Name(\d+)? (\([^\[]+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["last_names"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Name(\d+)? (\([^\[]+\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["last_names"])

        mo = re.match("\[\*\*Name Initial (\([^\[]*\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                firstname = random.choice(self.lists_replacements["all_first_names"])
                name = random.choice(self.lists_replacements["last_names"])
                self.placeholder_mapping[mo.group(0)] = "{}{}".format(firstname[0:1], name[0:1])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Name Initial (\([^\[]*\) )?\*\*\]", placeholder)
        if mo:
            firstname = random.choice(self.lists_replacements["all_first_names"])
            name = random.choice(self.lists_replacements["last_names"])
            return "{}{}".format(firstname[0:1], name[0:1])

        mo = re.match("\[\*\*Numeric Identifier \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 10000))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Numeric Identifier \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 10000))

        mo = re.match("\[\*\*Pager number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["phone_numbers"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Pager number \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["phone_numbers"])

        mo = re.match("\[\*\*Provider Number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["phone_numbers"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Provider Number \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["phone_numbers"])

        mo = re.match("\[\*\*Serial Number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}-{}-{}".format(
                    str(random.randint(1, 10000)),
                    str(random.randint(1, 10000)),
                    str(random.randint(1, 10000))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Serial Number \*\*\]", placeholder)
        if mo:
            return "{}-{}-{}".format(
                    str(random.randint(1, 10000)),
                    str(random.randint(1, 10000)),
                    str(random.randint(1, 10000))
                )

        mo = re.match("\[\*\*Social Security Number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["ssn"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Social Security Number \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["ssn"])

        mo = re.match("\[\*\*State \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["states"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*State \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["states"])

        mo = re.match("\[\*\*Street Address(\(\d+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["addresses"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Street Address(\(\d+\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["addresses"])

        mo = re.match("\[\*\*Telephone/Fax (\(\d+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["phone_numbers"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Telephone/Fax (\(\d+\) )?\*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["phone_numbers"])

        mo = re.match("\[\*\*Unit Number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 10000))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Unit Number \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 10000))

        mo = re.match("\[\*\*(\d\d\d\d-\d?\d-\d?\d)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = mo.group(1)
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Year \((\d+) digits\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1950, 2016))[int(mo.group(1)):]
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Year \((\d+) digits\) \*\*\]", placeholder)
        if mo:
            return str(random.randint(1950, 2016))[int(mo.group(1)):]

        mo = re.match("\[\*\*Year/Month/Day \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}/{}".format(
                    str(random.randint(1950, 2016)),
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Year/Month/Day \*\*\]", placeholder)
        if mo:
            return "{}/{}/{}".format(
                    str(random.randint(1950, 2016)),
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31))
                )

        mo = re.match("\[\*\*((January|February|March|April|May|June|July|August|"
                      "September|October|November|December) \d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = mo.group(1)
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Name Prefix \(Prefixes\) \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(['Ms', 'Miss', 'Mrs', 'Mr', 'Dr', 'Prof'])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Name Prefix \(Prefixes\) \*\*\]", placeholder)
        if mo:
            return random.choice(['Ms', 'Miss', 'Mrs', 'Mr', 'Dr', 'Prof'])

        mo = re.match("\[\*\*PO Box \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "PO BOX {}".format(
                    str(random.randint(1, 1000))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*PO Box \*\*\]", placeholder)
        if mo:
            return "PO BOX {}".format(
                    str(random.randint(1, 1000))
                )

        mo = re.match("\[\*\*Year/Month \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}".format(
                    str(random.randint(1950, 2016)),
                    str(random.randint(1, 12))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Year/Month \*\*\]", placeholder)
        if mo:
            return "{}/{}".format(
                    str(random.randint(1950, 2016)),
                    str(random.randint(1, 12))
                )

        mo = re.match("\[\*\*Month Day Year (\(\d+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{} {} {}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31)),
                    str(random.randint(1950, 2016))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month Day Year (\(\d+\) )?\*\*\]", placeholder)
        if mo:
            return "{} {} {}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1, 31)),
                    str(random.randint(1950, 2016))
                )

        mo = re.match("\[\*\*Month Year \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{} {}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1950, 2016))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Month Year \*\*\]", placeholder)
        if mo:
            return "{} {}".format(
                    str(random.randint(1, 12)),
                    str(random.randint(1950, 2016))
                )

        mo = re.match("\[\*\*Day Month \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{} {}".format(
                    str(random.randint(1, 31)),
                    str(random.randint(1, 12))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Day Month \*\*\]", placeholder)
        if mo:
            return "{} {}".format(
                    str(random.randint(1, 31)),
                    str(random.randint(1, 12))
                )

        mo = re.match("\[\*\*Day Month Year (\(\d+\) )?\d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{} {} {}".format(
                    str(random.randint(1, 31)),
                    str(random.randint(1, 12)),
                    str(random.randint(1950, 2016))
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Day Month Year (\(\d+\) )?\*\*\]", placeholder)
        if mo:
            return "{} {} {}".format(
                    str(random.randint(1, 31)),
                    str(random.randint(1, 12)),
                    str(random.randint(1950, 2016))
                )

        mo = re.match("\[\*\*State/Zipcode \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = str(random.randint(1, 99999))
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*State/Zipcode \*\*\]", placeholder)
        if mo:
            return str(random.randint(1, 99999))

        mo = re.match("\[\*\*Hospital Unit Number \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["phone_numbers"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Hospital Unit Number \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["phone_numbers"])

        mo = re.match("\[\*\*University/College \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["colleges"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*University/College \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["colleges"])

        mo = re.match("\[\*\*Hospital Ward Name \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["wards_units"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Hospital Ward Name \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["wards_units"])

        mo = re.match("\[\*\*Hospital Unit Name \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["wards_units"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Hospital Unit Name \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["wards_units"])

        mo = re.match("\[\*\*Wardname \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["wards_units"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*Wardname \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["wards_units"])

        mo = re.match("\[\*\*URL \d+\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = random.choice(self.lists_replacements["websites"])
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*URL \*\*\]", placeholder)
        if mo:
            return random.choice(self.lists_replacements["websites"])

        mo = re.match("\[\*\* \d+\*\*\]", placeholder)
        if mo:
            return ''

        mo = re.match("\[\*\*\s\*\*\]", placeholder)
        if mo:
            return ''

        mo = re.match("\[\*\*(\d+)-/(\d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}".format(
                    mo.group(1),
                    mo.group(2)
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*(\d+)/(\d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}".format(
                    mo.group(1),
                    mo.group(2)
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*(\d+)-(\d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}-{}".format(
                    mo.group(1),
                    mo.group(2)
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*-(\d+)/(\d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = "{}/{}".format(
                    mo.group(1),
                    mo.group(2)
                )
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*(\d+-\d+-\d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = mo.group(1)
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*(\d+)\*\*\]", placeholder)
        if mo:
            if mo.group(0) not in self.placeholder_mapping:
                self.placeholder_mapping[mo.group(0)] = mo.group(1)
            return self.placeholder_mapping[mo.group(0)]

        mo = re.match("\[\*\*[^\[]*\*\*\]", placeholder)
        if mo:
            return ''
            # return placeholder

    @staticmethod
    def _build_date_range():

        year_begin = random.randint(1950, 2016)
        month_begin = random.randint(1, 12)
        day_begin = random.randint(1, 28)

        year_end = random.randint(year_begin, year_begin + 2)
        if year_end > year_begin:
            month_end = random.randint(1, 12)
            day_end = random.randint(1, 28)
        else:
            month_end = random.randint(month_begin, 12)
            if month_end > month_begin:
                day_end = random.randint(1, 28)
            else:
                day_end = random.randint(day_begin, 28)

        return year_begin, month_begin, day_begin, year_end, month_end, day_end


def replace_placeholders(corpus_path, output_path, list_path):

    # Variable where lists of replacements will be stored
    list_sub = {
        'addresses': [],
        'last_names': [],
        'first_names_male': [],
        'first_names_female': [],
        'phone_numbers': [],
        'companies': [],
        'countries': [],
        'emails': [],
        'holidays': [],
        'hospitals': [],
        'locations': [],
        'months': ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                   'October', 'November', 'December'],
        'ssn': [],
        'states': [],
        'colleges': [],
        'wards_units': [],
        'websites': []
    }

    logging.info("Loading lists")

    with open(os.path.join(os.path.abspath(list_path), "www.randomlists.com", "addresses_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:

            if re.match("^$", line):
                continue

            list_sub["addresses"].append(line.rstrip())

    logging.info("* Postal addresses: {} [{} ...]".format(
        len(list_sub["addresses"]),
        list_sub["addresses"][0]
    ))

    regex_name = re.compile("^(.*)\d+\.\d+\s+\d+\.\d+\s+\d+$")

    with open(os.path.join(os.path.abspath(list_path), "1990_US_CENSUS", "dist.all.last"), "r",
              encoding="UTF-8") as input_file:
        for line in input_file:
            match_name = regex_name.match(line)
            if match_name:
                list_sub["last_names"].append(match_name.group(1).rstrip())

    logging.info("* Last names: {} [{} ...]".format(
        len(list_sub["last_names"]),
        ", ".join(list_sub["last_names"][:5])
    ))

    with open(os.path.join(os.path.abspath(list_path), "1990_US_CENSUS", "dist.male.first"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            match_name = regex_name.match(line)
            if match_name:
                list_sub["first_names_male"].append(match_name.group(1).rstrip())

    logging.info("* Male first names: {} [{} ...]".format(
        len(list_sub["first_names_male"]),
        ", ".join(list_sub["first_names_male"][:5])
    ))

    with open(os.path.join(os.path.abspath(list_path), "1990_US_CENSUS", "dist.female.first"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            match_name = regex_name.match(line)
            if match_name:
                list_sub["first_names_female"].append(match_name.group(1).rstrip())

    logging.info("* Female first names: {} [{} ...]".format(
        len(list_sub["first_names_female"]),
        ", ".join(list_sub["first_names_female"][:5])
    ))

    with open(os.path.join(os.path.abspath(list_path), "generatedata.com", "phone_numbers_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["phone_numbers"].append(line.rstrip())

    logging.info("* Phone numbers: {} [{} ...]".format(
        len(list_sub["phone_numbers"]),
        ", ".join(list_sub["phone_numbers"][:2])
    ))

    with open(os.path.join(os.path.abspath(list_path), "generatedata.com", "companies_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["companies"].append(line.rstrip())

    logging.info("* Companies: {} [{} ...]".format(
        len(list_sub["companies"]),
        ", ".join(list_sub["companies"][:2])
    ))

    with open(os.path.join(os.path.abspath(list_path), "www.countries-list.info", "countries.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["countries"].append(line.rstrip())

    logging.info("* Countries: {} [{} ...]".format(
        len(list_sub["countries"]),
        ", ".join(list_sub["countries"][:4])
    ))

    with open(os.path.join(os.path.abspath(list_path), "generatedata.com", "emails_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["emails"].append(line.rstrip())

    logging.info("* Emails: {} [{} ...]".format(
        len(list_sub["emails"]),
        ", ".join(list_sub["emails"][:2])
    ))

    with open(os.path.join(os.path.abspath(list_path), "misc", "holidays.lst"),
              "r", encoding="UTF-8") as input_file:
        temp_set = set()
        for line in input_file:
            if re.match("^$", line):
                continue
            temp_set.add(line.rstrip())
        list_sub["holidays"] = sorted(list(temp_set))  # Changed by dalgu90
#         list_sub["holidays"] = list(temp_set)

    logging.info("* Holiday names: {} [{} ...]".format(
        len(list_sub["holidays"]),
        ", ".join(list_sub["holidays"][:2])
    ))

    with open(os.path.join(os.path.abspath(list_path), "data.medicare.gov", "hospitals.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["hospitals"].append(line.rstrip())

    logging.info("* Hospital names: {} [{} ...]".format(
        len(list_sub["hospitals"]),
        ", ".join(list_sub["hospitals"][:2])
    ))

    with open(os.path.join(os.path.abspath(list_path), "generatedata.com", "locations_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["locations"].append(line.rstrip())

    logging.info("* Location names: {} [{} ...]".format(
        len(list_sub["locations"]),
        ", ".join(list_sub["locations"][:3])
    ))

    with open(os.path.join(os.path.abspath(list_path), "generatedata.com", "social_security_numbers_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["ssn"].append(line.rstrip())

    logging.info("* SSN: {} [{} ...]".format(
        len(list_sub["ssn"]),
        ", ".join(list_sub["ssn"][:3])
    ))

    with open(os.path.join(os.path.abspath(list_path), "misc", "US_states.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["states"].append(line.rstrip())

    logging.info("* US_States: {} [{} ...]".format(
        len(list_sub["states"]),
        ", ".join(list_sub["states"][:3])
    ))

    with open(os.path.join(os.path.abspath(list_path), "talk.collegeconfidential.com", "colleges.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["colleges"].append(line.rstrip())

    logging.info("* Colleges: {} [{} ...]".format(
        len(list_sub["colleges"]),
        ", ".join(list_sub["colleges"][:3])
    ))

    with open(os.path.join(os.path.abspath(list_path), "misc", "hospital_wards_units.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["wards_units"].append(line.rstrip())

    logging.info("* Wards & Units: {} [{} ...]".format(
        len(list_sub["wards_units"]),
        ", ".join(list_sub["wards_units"][:3])
    ))

    with open(os.path.join(os.path.abspath(list_path), "generatedata.com", "websites_random.lst"),
              "r", encoding="UTF-8") as input_file:
        for line in input_file:
            if re.match("^$", line):
                continue
            list_sub["websites"].append(line.rstrip())

    logging.info("* Websites: {} [{} ...]".format(
        len(list_sub["websites"]),
        ", ".join(list_sub["websites"][:3])
    ))

    list_sub["all_first_names"] = list_sub["first_names_female"] + list_sub["first_names_male"]

    logging.info("* Combining female and male first names: {} [{} ...]".format(
        len(list_sub["all_first_names"]),
        ", ".join(list_sub["all_first_names"][:3])
    ))

    logging.info("Creating mapper")
    mapper = PlaceholderMapper(list_sub)

    logging.info("Computing number of files to process")

    nb_files = 0

    for root, dirs, files in os.walk(os.path.abspath(corpus_path)):
        for filename in files:
            if re.match("^.*\.txt$", filename):
                nb_files += 1

    processed = 0

    logging.info("Replacing placeholders. This can take a long time...")

    for root, dirs, files in os.walk(os.path.abspath(corpus_path)):
        for filename in sorted(files):
            if re.match(".*\.txt", filename):

                source_file = os.path.join(root, filename)
                subdir = remove_abs(re.sub(os.path.abspath(corpus_path), "", root))

                target_path = os.path.join(os.path.abspath(output_path), subdir)
                target_file = os.path.join(target_path, filename)

                ensure_dir(target_path)

                content = open(source_file, "r", encoding="UTF-8").read()
                content_modified = ''

                start = 0

                for mo in re.finditer("\[\*\*[^\[]*\*\*\]", content):

                    replacement = mapper.get_mapping(mo.group(0))

                    content_modified += content[start: mo.start()]
                    content_modified += replacement

                    start = mo.end()

                if start < len(content):
                    content_modified += content[start: len(content)]

                with open(target_file, "w", encoding="UTF-8") as output_file:
                    output_file.write(content_modified)

                processed += 1
                if processed % 1000 == 0 or processed == nb_files:
                    logging.info("Processed: {}/{} ({}%)".format(
                        processed, nb_files, round(float(processed/nb_files) * 100, 2)
                    ))

    logging.info("Done !")
