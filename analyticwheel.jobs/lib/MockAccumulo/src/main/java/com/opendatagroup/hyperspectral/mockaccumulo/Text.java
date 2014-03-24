package com.opendatagroup.hyperspectral.mockaccumulo;

public class Text {
    public String text = null;

    public Text(String _text) {
        text = _text;
    }

    @Override public String toString() {
        return text;
    }
}
