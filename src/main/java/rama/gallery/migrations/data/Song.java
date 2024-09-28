package rama.gallery.migrations.data;

import com.rpl.rama.RamaSerializable;

import java.util.List;

public class Song implements RamaSerializable {
    public String name;
    public List<String> featuredArtists;

    public Song(String name, List<String> featuredArtists) {
        this.name = name;
        this.featuredArtists = featuredArtists;
    }
}
