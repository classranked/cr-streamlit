from __future__ import annotations

import csv
import json
import re
import zipfile
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


SSA_OFFICIAL_FIRST_NAMES = [
    "Liam", "Noah", "Oliver", "Theodore", "James", "Henry", "Mateo", "Elijah", "Lucas", "William",
    "Olivia", "Emma", "Amelia", "Charlotte", "Mia", "Sophia", "Isabella", "Evelyn", "Ava", "Sofia",
    "Noah", "Liam", "Jacob", "William", "Mason", "Ethan", "Michael", "Alexander", "James", "Elijah",
    "Benjamin", "Daniel", "Aiden", "Logan", "Jayden", "Matthew", "Lucas", "Jackson", "David", "Joseph",
    "Anthony", "Joshua", "Andrew", "Gabriel", "Samuel", "Christopher", "John", "Dylan", "Isaac", "Ryan",
    "Nathan", "Carter", "Caleb", "Luke", "Christian", "Hunter", "Henry", "Owen", "Landon", "Jack",
    "Wyatt", "Jonathan", "Eli", "Isaiah", "Sebastian", "Jaxon", "Brayden", "Gavin", "Levi", "Aaron",
    "Oliver", "Jordan", "Nicholas", "Evan", "Connor", "Charles", "Jeremiah", "Cameron", "Adrian", "Thomas",
    "Robert", "Tyler", "Colton", "Austin", "Jace", "Angel", "Dominic", "Josiah", "Brandon", "Ayden",
    "Kevin", "Zachary", "Parker", "Blake", "Jose", "Chase", "Grayson", "Jason", "Ian", "Bentley",
    "Adam", "Xavier", "Cooper", "Justin", "Nolan", "Hudson", "Easton", "Jase", "Carson", "Nathaniel",
    "Jaxson", "Kayden", "Brody", "Lincoln", "Luis", "Tristan", "Damian", "Camden", "Juan", "Max",
    "Myles", "Santiago", "Ezekiel", "Vincent", "Micah", "Maverick", "Bryson", "Greyson", "Asher", "Cole",
    "Declan", "Braxton", "Ryder", "Diego", "Antonio", "Jesus", "Miguel", "Milo", "Steven", "Jasper",
    "Axel", "Jason", "Aidan", "Eric", "Messiah", "George", "Emiliano", "Calvin", "Bryce", "Elliot",
    "Maxwell", "Ivan", "Kingston", "Juan", "Maddox", "Justin", "Carlos", "Kaiden", "Luis", "Kaiden",
    "Peyton", "Giovanni", "Mackenzie", "Diego", "Vincent", "Legend", "Malachi", "Leonardo", "Avery", "Kai",
    "Emma", "Olivia", "Sophia", "Isabella", "Ava", "Mia", "Emily", "Abigail", "Madison", "Charlotte",
    "Harper", "Sofia", "Avery", "Elizabeth", "Amelia", "Evelyn", "Ella", "Chloe", "Victoria", "Aubrey",
    "Grace", "Zoey", "Natalie", "Addison", "Lillian", "Brooklyn", "Hannah", "Lily", "Layla", "Scarlett",
    "Aria", "Zoe", "Samantha", "Aurora", "Ellie", "Aaliyah", "Claire", "Violet", "Stella", "Lucy",
    "Anna", "Mila", "Paisley", "Savannah", "Allison", "Sarah", "Skylar", "Nora", "Leah", "Elizabeth",
    "Hazel", "Audrey", "Ariana", "Sophie", "Bella", "Aaliyah", "Sadie", "Peyton", "Julia", "Genesis",
    "Kennedy", "Madelyn", "Ruby", "Serenity", "Willow", "Naomi", "Eva", "Kinsley", "Caroline", "Alice",
    "Valentina", "Quinn", "Gabriella", "Maya", "Samantha", "Sarah", "Ariana", "Mackenzie", "Nevaeh", "Jade",
    "Cora", "Arianna", "Kaylee", "Lydia", "Aubree", "Eliana", "Piper", "Rylee", "Taylor", "Brielle",
    "Lyla", "Clara", "Hadley", "Melanie", "Madeline", "Bailey", "Delilah", "Adeline", "Vivian", "Camila",
    "Gianna", "Molly", "Reagan", "Ashley", "Ryleigh", "Faith", "Rose", "Kylie", "Leilani", "Isabelle",
    "Juniper", "Isla", "Laila", "Makayla", "Khloe", "Raelynn", "Arya", "Aliyah", "Maria", "Londyn",
    "Ariella", "Eliza", "Laila", "Morgan", "Piper", "Sydney", "Jocelyn", "Trinity", "London", "Lauren",
    "Brianna", "Lydia", "Rylee", "Molly", "Brielle", "Jade", "Alexandra", "Taylor", "Margaret", "Brooke",
    "James", "John", "Robert", "Michael", "William", "David", "Richard", "Charles", "Joseph", "Thomas",
    "Christopher", "Daniel", "Paul", "Mark", "Donald", "George", "Kenneth", "Steven", "Edward", "Brian",
    "Ronald", "Anthony", "Kevin", "Jason", "Matthew", "Gary", "Timothy", "Jose", "Larry", "Jeffrey",
    "Frank", "Scott", "Eric", "Stephen", "Andrew", "Raymond", "Gregory", "Joshua", "Jerry", "Dennis",
    "Walter", "Patrick", "Peter", "Harold", "Douglas", "Henry", "Carl", "Arthur", "Ryan", "Roger",
    "Joe", "Juan", "Jack", "Albert", "Jonathan", "Justin", "Terry", "Gerald", "Keith", "Samuel",
    "Willie", "Ralph", "Lawrence", "Nicholas", "Roy", "Benjamin", "Bruce", "Brandon", "Adam", "Harry",
    "Fred", "Wayne", "Billy", "Steve", "Louis", "Jeremy", "Aaron", "Randy", "Howard", "Eugene",
    "Carlos", "Russell", "Bobby", "Victor", "Martin", "Ernest", "Phillip", "Todd", "Jesse", "Craig",
    "Alan", "Shawn", "Clarence", "Sean", "Philip", "Chris", "Johnny", "Earl", "Jimmy", "Antonio",
    "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen",
    "Nancy", "Lisa", "Margaret", "Betty", "Sandra", "Ashley", "Kimberly", "Emily", "Donna", "Michelle",
    "Dorothy", "Carol", "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura", "Cynthia",
    "Kathleen", "Amy", "Shirley", "Angela", "Helen", "Anna", "Brenda", "Pamela", "Nicole", "Emma",
    "Samantha", "Katherine", "Christine", "Debra", "Rachel", "Catherine", "Carolyn", "Janet", "Ruth", "Maria",
    "Heather", "Diane", "Virginia", "Julie", "Joyce", "Victoria", "Olivia", "Kelly", "Christina", "Lauren",
    "Joan", "Evelyn", "Judith", "Megan", "Cheryl", "Andrea", "Hannah", "Martha", "Jacqueline", "Frances",
    "Gloria", "Ann", "Teresa", "Kathryn", "Sara", "Janice", "Jean", "Alice", "Madison", "Doris",
    "Abigail", "Julia", "Judy", "Grace", "Denise", "Amber", "Marilyn", "Beverly", "Danielle", "Theresa",
    "Sophia", "Marie", "Diana", "Brittany", "Natalie", "Isabella", "Charlotte", "Rose", "Alexis", "Kayla",
]

FALLBACK_FIRST_NAMES = [
    "Aubrey", "Ellie", "Savannah", "Claire", "Skylar", "Bella", "Genesis", "Kennedy", "Madelyn", "Adeline",
    "Caroline", "Riley", "Peyton", "Melanie", "Autumn", "Serenity", "Faith", "Ariel", "Athena", "Lydia",
    "Mariana", "Eleanor", "Adriana", "Liliana", "Delilah", "Valentina", "Josephine", "Gabriella", "Brooklyn", "Luna",
    "Cora", "Kinsley", "Allison", "Eliana", "Sophie", "Violet", "Sadie", "Madeline", "Kylie", "Reagan",
    "Arya", "Rylee", "Margot", "Morgan", "Sydney", "Jenna", "Rosalie", "Miriam", "Daniela", "Anastasia",
    "Noelle", "Veronica", "Fatima", "Helena", "Lucille", "Esmeralda", "Elisa", "Cecilia", "Fernanda", "Amina",
    "Alina", "Selena", "Imani", "Talia", "Nadia", "Kiara", "Sasha", "Monica", "April", "Mckenna",
    "Wesley", "Miles", "Cole", "Micah", "Silas", "Hudson", "Ezra", "Julian", "Leo", "Isaac",
    "Lincoln", "Maverick", "Mateo", "Luca", "Asher", "Carter", "Wyatt", "Grayson", "Caleb", "Jayden",
    "Roman", "Theo", "Jaxon", "Kai", "Declan", "Axel", "Ryder", "Emmett", "Sawyer", "Kingston",
]

EDGE_CASE_NAMES = [
    ("José", "García", "accented-spanish"),
    ("María", "Fernández", "accented-spanish"),
    ("Ana Sofía", "Ruiz-Ortega", "compound-given-hyphenated-surname"),
    ("Renée", "O'Connor", "apostrophe-accent"),
    ("Björn", "Åkesson", "nordic"),
    ("Chloë", "D'Amico", "diaeresis-apostrophe"),
    ("François", "L'Écuyer", "french-apostrophe"),
    ("Jiří", "Novák", "central-european"),
    ("Zoë", "Smith-Jones", "hyphenated"),
    ("Lucía", "de la Cruz", "multipart-surname"),
    ("Saoirse", "Ní Bhraonáin", "irish"),
    ("Mónica", "Peña", "tilde"),
    ("Noël", "Bélanger", "french"),
    ("Amina", "El-Hadid", "arabic-hyphenated"),
    ("İrem", "Yılmaz", "turkish"),
    ("Łukasz", "Kowalski", "polish"),
    ("Sébastien", "Dubois", "acute"),
    ("Inés", "Martín del Campo", "multipart"),
    ("Maëlle", "Brunet", "ligature"),
    ("Yara", "Al-Sayed", "hyphenated-middle-east"),
]

SUBJECTS = [
    ("COMPSCI", "Computer Science", "School of Computing", "Engineering and Technology", "College of Engineering", "Computer Science"),
    ("DATA", "Data Science", "School of Computing", "Engineering and Technology", "College of Engineering", "Data Science"),
    ("MATH", "Mathematics", "Mathematical Sciences", "Natural Sciences", "College of Arts and Sciences", "Mathematics"),
    ("STAT", "Statistics", "Mathematical Sciences", "Natural Sciences", "College of Arts and Sciences", "Statistics"),
    ("BIO", "Biology", "Life Sciences", "Natural Sciences", "College of Arts and Sciences", "Biology"),
    ("CHEM", "Chemistry", "Physical Sciences", "Natural Sciences", "College of Arts and Sciences", "Chemistry"),
    ("PHYS", "Physics", "Physical Sciences", "Natural Sciences", "College of Arts and Sciences", "Physics"),
    ("ENG", "English", "Humanities", "Arts and Letters", "College of Arts and Sciences", "English"),
    ("HIST", "History", "Humanities", "Arts and Letters", "College of Arts and Sciences", "History"),
    ("PHIL", "Philosophy", "Humanities", "Arts and Letters", "College of Arts and Sciences", "Philosophy"),
    ("SPAN", "Spanish", "Languages and Cultures", "Arts and Letters", "College of Arts and Sciences", "Spanish"),
    ("FREN", "French", "Languages and Cultures", "Arts and Letters", "College of Arts and Sciences", "French"),
    ("ARAB", "Arabic", "Languages and Cultures", "Arts and Letters", "College of Arts and Sciences", "Arabic"),
    ("CHIN", "Chinese", "Languages and Cultures", "Arts and Letters", "College of Arts and Sciences", "Chinese"),
    ("ECON", "Economics", "Social Sciences", "Business and Society", "College of Arts and Sciences", "Economics"),
    ("PSYC", "Psychology", "Behavioral Sciences", "Health and Society", "College of Arts and Sciences", "Psychology"),
    ("SOC", "Sociology", "Social Sciences", "Business and Society", "College of Arts and Sciences", "Sociology"),
    ("POLS", "Political Science", "Social Sciences", "Business and Society", "College of Arts and Sciences", "Political Science"),
    ("BUS", "Business Administration", "Business Core", "Management and Markets", "School of Business", "Business Administration"),
    ("ACCT", "Accounting", "Business Core", "Management and Markets", "School of Business", "Accounting"),
    ("FIN", "Finance", "Business Core", "Management and Markets", "School of Business", "Finance"),
    ("MKTG", "Marketing", "Business Core", "Management and Markets", "School of Business", "Marketing"),
    ("MGMT", "Management", "Business Core", "Management and Markets", "School of Business", "Management"),
    ("NURS", "Nursing", "Clinical Sciences", "Health Professions", "College of Health Sciences", "Nursing"),
    ("PHARM", "Pharmacy", "Clinical Sciences", "Health Professions", "College of Health Sciences", "Pharmacy"),
    ("PUBH", "Public Health", "Population Health", "Health Professions", "College of Health Sciences", "Public Health"),
    ("EDU", "Education", "Teacher Preparation", "Education and Human Development", "College of Education", "Education"),
    ("COUN", "Counseling", "Teacher Preparation", "Education and Human Development", "College of Education", "Counseling"),
    ("MUS", "Music", "Performing Arts", "Creative Arts", "College of Fine Arts", "Music"),
    ("ART", "Art", "Visual Arts", "Creative Arts", "College of Fine Arts", "Art"),
    ("THEA", "Theatre", "Performing Arts", "Creative Arts", "College of Fine Arts", "Theatre"),
    ("ARCH", "Architecture", "Built Environment", "Design and Innovation", "College of Design", "Architecture"),
    ("URBD", "Urban Design", "Built Environment", "Design and Innovation", "College of Design", "Urban Design"),
    ("CIVIL", "Civil Engineering", "Engineering Disciplines", "Engineering and Technology", "College of Engineering", "Civil Engineering"),
    ("MECH", "Mechanical Engineering", "Engineering Disciplines", "Engineering and Technology", "College of Engineering", "Mechanical Engineering"),
    ("ELEC", "Electrical Engineering", "Engineering Disciplines", "Engineering and Technology", "College of Engineering", "Electrical Engineering"),
    ("CHEG", "Chemical Engineering", "Engineering Disciplines", "Engineering and Technology", "College of Engineering", "Chemical Engineering"),
    ("INDU", "Industrial Engineering", "Engineering Disciplines", "Engineering and Technology", "College of Engineering", "Industrial Engineering"),
    ("LAW", "Law", "Professional Studies", "Law and Policy", "School of Law", "Law"),
    ("CJUS", "Criminal Justice", "Professional Studies", "Law and Policy", "School of Law", "Criminal Justice"),
    ("MED", "Medicine", "Clinical Sciences", "Health Professions", "School of Medicine", "Medicine"),
    ("DENT", "Dentistry", "Clinical Sciences", "Health Professions", "School of Dentistry", "Dentistry"),
    ("ENV", "Environmental Studies", "Earth Systems", "Natural Sciences", "College of Arts and Sciences", "Environmental Studies"),
    ("GEOG", "Geography", "Earth Systems", "Natural Sciences", "College of Arts and Sciences", "Geography"),
    ("ANTH", "Anthropology", "Social Sciences", "Business and Society", "College of Arts and Sciences", "Anthropology"),
    ("LING", "Linguistics", "Languages and Cultures", "Arts and Letters", "College of Arts and Sciences", "Linguistics"),
    ("JOUR", "Journalism", "Media and Communication", "Creative Arts", "School of Communication", "Journalism"),
    ("COMM", "Communication", "Media and Communication", "Creative Arts", "School of Communication", "Communication"),
    ("IS", "Information Systems", "Business Core", "Management and Markets", "School of Business", "Information Systems"),
]

CAMPUSES = ["Boston", "San Jose", "Austin", "Chicago", "Online", "Washington DC"]
SESSIONS = ["Fall 1", "Fall 2", "Spring 1", "Spring 2", "Summer 1", "Summer 2", "Full Term"]
DELIVERY_METHODS = ["On-Campus", "Online", "Hybrid", "HyFlex"]
COURSE_TYPES = ["Lecture", "Lab", "Seminar", "Practicum", "Independent Study", "Special Topics", "Dissertation"]

TITLE_TEMPLATES = {
    "Computer Science": [
        "Introduction to Programming",
        "Data Structures",
        "Algorithms",
        "Computer Systems",
        "Database Systems",
        "Operating Systems",
        "Software Engineering",
        "Artificial Intelligence",
        "Machine Learning",
        "Human-Computer Interaction",
        "Cloud Computing",
        "Distributed Systems",
        "Computer Networks",
        "Cybersecurity Fundamentals",
        "Capstone in Computer Science",
    ],
    "Data Science": [
        "Foundations of Data Science",
        "Data Wrangling",
        "Statistical Learning",
        "Data Visualization",
        "Applied Machine Learning",
        "Big Data Platforms",
        "Responsible AI",
        "Deep Learning Applications",
        "Data Ethics and Governance",
        "Natural Language Processing",
        "Experiment Design",
        "Predictive Modeling",
    ],
    "Spanish": [
        "Introducción al Español Académico",
        "Literatura Hispana Contemporánea",
        "Español para los Negocios",
        "Cultura y Sociedad Latinoamericana",
        "Traducción e Interpretación",
        "Escritura Avanzada en Español",
    ],
    "French": [
        "Français Élémentaire I",
        "Français Intermédiaire",
        "Civilisation Française",
        "Littérature et Société",
        "Français pour les Professions",
        "Atelier d'Écriture",
    ],
    "Arabic": [
        "Arabic Language and Culture I",
        "Modern Arabic Media",
        "Arabic for Global Affairs",
        "Intermediate Arabic Conversation",
        "Arabic Literature in Translation",
    ],
    "Chinese": [
        "Mandarin Chinese I",
        "Business Chinese Communication",
        "Chinese Media and Society",
        "Intermediate Spoken Chinese",
        "Chinese Literature and Film",
    ],
}

DEFAULT_TITLES = [
    "Foundations of {program}",
    "{program} in Practice",
    "Research Methods in {program}",
    "Contemporary Issues in {program}",
    "Seminar in {program}",
    "Applied {program}",
    "{program} Capstone",
    "{program} Internship",
    "Topics in {program}",
    "Advanced {program}",
    "Leadership in {program}",
    "Global Perspectives in {program}",
]


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_name_rows(values: list[str]) -> list[dict]:
    total = len(values)
    rows = []
    for index, name in enumerate(values[:500], start=1):
        rows.append({"name": name, "weight": total - index + 1})
    return rows


def extend_with_local_names(values: list[str], target_count: int = 500) -> list[str]:
    result = list(dict.fromkeys(values))
    source_path = Path("/usr/share/dict/propernames")
    if source_path.exists():
        for raw_name in source_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            name = raw_name.strip()
            if not re.fullmatch(r"[A-Za-z][A-Za-z' -]{1,30}", name):
                continue
            if name not in result:
                result.append(name)
            if len(result) >= target_count:
                break
    return result[:target_count]


def dedupe_preserve(values: list[str]) -> list[str]:
    result = []
    seen = set()
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def load_official_first_names(target_count: int = 500) -> list[str]:
    official = dedupe_preserve(SSA_OFFICIAL_FIRST_NAMES)
    if len(official) < target_count:
        official = dedupe_preserve(official + FALLBACK_FIRST_NAMES)
    if len(official) < target_count:
        official = extend_with_local_names(official, target_count=target_count)
    return official[:target_count]


def normalize_surname(name: str) -> str:
    value = str(name).strip().lower().title()
    value = re.sub(r"\bMc([a-z])", lambda m: f"Mc{m.group(1).upper()}", value)
    value = re.sub(r"\bMac([a-z])", lambda m: f"Mac{m.group(1).upper()}", value)
    return value


def load_official_last_names(target_count: int = 500) -> list[str]:
    census_zip_path = Path("/tmp/census_surnames.zip")
    if census_zip_path.exists():
        with zipfile.ZipFile(census_zip_path) as archive:
            with archive.open("Names_2010Census.csv") as handle:
                df = pd.read_csv(handle)
        df = df[df["name"].astype(str).str.upper() != "ALL OTHER NAMES"]
        df = df.sort_values("rank").head(target_count)
        return [normalize_surname(name) for name in df["name"].tolist()]
    existing = DATA_DIR / "common_last_names.csv"
    if existing.exists():
        df = pd.read_csv(existing)
        return df["name"].astype(str).head(target_count).tolist()
    return []


def course_levels(course_number: int) -> str:
    bucket = (course_number // 100) * 100
    return f"{bucket} Level"


def build_catalog_rows() -> list[dict]:
    rows = []
    university = "ClassRanked University"
    course_numbers = [
        101, 102, 110, 120, 130, 140,
        201, 202, 210, 220, 230, 240,
        301, 302, 310, 320, 330, 340,
        401, 402, 410, 420, 430, 440,
        501, 502, 510, 520, 530, 540,
        601, 602, 610, 620, 630, 640,
        701, 702, 710, 720, 730, 740,
        780, 781, 790, 791, 795, 799,
    ]
    for subject_code, department, division, college_division, college, program in SUBJECTS:
        titles = TITLE_TEMPLATES.get(program, [])
        if not titles:
            titles = [template.format(program=program) for template in DEFAULT_TITLES]
        for index, course_number in enumerate(course_numbers):
            title = titles[index % len(titles)]
            campus = CAMPUSES[index % len(CAMPUSES)]
            session = SESSIONS[index % len(SESSIONS)]
            course_type = COURSE_TYPES[index % len(COURSE_TYPES)]
            delivery_method = DELIVERY_METHODS[index % len(DELIVERY_METHODS)]
            rows.append(
                {
                    "subject_code": subject_code,
                    "course_number": str(course_number),
                    "course_id": f"{subject_code}{course_number}",
                    "title": title,
                    "academic_unit_type": "Course",
                    "parent_academic_unit": subject_code,
                    "department": department,
                    "division": division,
                    "college": college,
                    "university": university,
                    "program": program,
                    "campus": campus,
                    "session": session,
                    "course_level": course_levels(course_number),
                    "course_type": course_type,
                    "delivery_method": delivery_method,
                }
            )
    return rows


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    first_names = load_official_first_names()
    last_names = load_official_last_names()

    write_csv(
        DATA_DIR / "common_first_names.csv",
        build_name_rows(first_names),
        ["name", "weight"],
    )
    write_csv(
        DATA_DIR / "common_last_names.csv",
        build_name_rows(last_names),
        ["name", "weight"],
    )
    write_csv(
        DATA_DIR / "edge_case_names.csv",
        [
            {"first_name": first_name, "last_name": last_name, "label": label}
            for first_name, last_name, label in EDGE_CASE_NAMES
        ],
        ["first_name", "last_name", "label"],
    )
    write_csv(
        DATA_DIR / "master_course_catalog.csv",
        build_catalog_rows(),
        [
            "subject_code",
            "course_number",
            "course_id",
            "title",
            "academic_unit_type",
            "parent_academic_unit",
            "department",
            "division",
            "college",
            "university",
            "program",
            "campus",
            "session",
            "course_level",
            "course_type",
            "delivery_method",
        ],
    )

    metadata = {
        "catalog_rows": len(build_catalog_rows()),
        "first_name_rows": len(first_names),
        "last_name_rows": len(last_names),
        "first_name_source": [
            "SSA official pages: 2024 top 10, 2010s decade rankings, and century rankings.",
            "A small fallback list may be used only if the official page-derived pool is below 500 unique names.",
        ],
        "last_name_source": [
            "U.S. Census Bureau 2010 Census surnames archive: Names_2010Census.csv",
        ],
        "notes": [
            "Synthetic large-university course catalog for SIS test data generation.",
            "Name corpora are local vendored seed datasets stored in the repo for deterministic generation and deployment.",
        ],
    }
    with open(DATA_DIR / "seed_metadata.json", "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)


if __name__ == "__main__":
    main()
