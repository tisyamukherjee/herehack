# Roundabout Analysis in Hamburg (HereHack)

## Overview
This project was developed for the HereHack hackathon and focuses on identifying roundabouts in Hamburg, Germany, using provided geographic data such as probe data, stop signs, traffic lights, and roundabout boundary areas.

## Key Features
- **Data Filtering**: Removes probe data points that are too close to stop signs or traffic lights (within 100 meters).
- **Roundabout Identification**: Uses probe data to identify locations with consistent movement patterns, which are likely roundabouts.
- **Visualization**: The final cleaned data is output to a CSV file for further GIS visualization.

## How It Works
1. **Input Data**: 
   - Probe data files containing GPS coordinates and timestamps.
   - Traffic data (stop signs, traffic lights) and roundabout boundary areas.

2. **Data Processing**:
   - The script filters out points too close to stop signs and traffic lights.
   - It checks for movement patterns in probe data to identify likely roundabout locations.

3. **Output**:
   - Cleaned data containing coordinates for potential roundabouts is saved as a CSV file.
