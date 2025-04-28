FROM quay.io/astronomer/astro-runtime:12.8.0

# Install en_core_web_sm for Spacy inside container
RUN python -m spacy download en_core_web_sm
