# Record colors for use throughout the app. Note that these colors are not
# visible to .streamlit/config.toml, which is responsible for storing the
# color values used and visible to the Streamlit framework.
# These colors are used wherever colors are implemented in python (i.e.
# graphs, visualizations, etc.).


class iMessageVisualizerColors(object):
    """
    Define colors for iMessage Visualizer.
    """
    def __init__(self):
        self.imessage_green = '#83cf83'
        self.imessage_blue = '#4598f6'
        self.imessage_purple = '#dfcbef'
        self.background_main = '#2b2b2b'
        self.xaxis_label = 'dimgray'  # For Altair visualizations
