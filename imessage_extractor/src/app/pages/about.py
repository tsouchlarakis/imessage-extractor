import streamlit as st


# pylint: disable=line-too-long
def write(data, logger):
    """
    Write the About page.
    """
    st.image('../../../graphics/about.png')

    st.markdown("""
    ## Contributions
    This is an open-source project and you're very welcome to contribute any and all ideas
    for improvements, bug fixes or new features as
    [pull requests](https://github.com/tsouchlarakis/imessage-extractor/pulls) or
    [issues](https://github.com/tsouchlarakis/imessage-extractor/issues) to the
    [source code](https://github.com/tsouchlarakis/imessage-extractor). For more details,
    see the repository's [README](https://github.com/tsouchlarakis/imessage-extractor/blob/main/README.rst).

    ## The Developer
    This project is developed by Andoni Sooklaris. You can learn more about me at
    [andonisooklaris.com](https://www.andonisooklaris.com). Feel free to reach out if
    you'd like to join the project as a developer. You can find my contact info on my website.
    """,
    unsafe_allow_html=True)
