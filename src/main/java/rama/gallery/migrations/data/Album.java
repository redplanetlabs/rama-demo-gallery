package rama.gallery.migrations.data;

import com.rpl.rama.RamaSerializable;

import java.util.List;
import java.util.Map;

public class Album implements RamaSerializable {
    public String artist;
    public String name;
    public List<String> songs;

    public Album(String artist, String name, List<String> songs) {
        this.artist = artist;
        this.name = name;
        this.songs = songs;
    }
}
