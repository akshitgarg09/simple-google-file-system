package com.gfs.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Chunk {

    String fileName;
    String nameSpace;
    int chunkIndex;
}
