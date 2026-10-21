import os

from hypothesis import settings

# Local runs stay fast; CI tries more random cases to search harder for counterexamples.
settings.register_profile("dev", max_examples=100)
settings.register_profile("ci", max_examples=1000)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
