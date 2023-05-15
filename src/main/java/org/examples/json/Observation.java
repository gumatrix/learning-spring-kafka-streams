package org.examples.json;

import lombok.Getter;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
public class Observation implements Serializable {

    @Serial
    private static final long serialVersionUID = 6019728453240986950L;

    private final List<Float> temperatures;
    private final List<Float> pressures;
    private final List<Float> windSpeeds;

    private int count;

    public Observation() {
        count = 0;
        temperatures = new ArrayList<>();
        pressures = new ArrayList<>();
        windSpeeds = new ArrayList<>();
    }

    public void record(Observer observer) {
        count += 1;
        temperatures.add(observer.temperature());
        pressures.add(observer.pressure());
        windSpeeds.add(observer.windSpeed());
    }
}
