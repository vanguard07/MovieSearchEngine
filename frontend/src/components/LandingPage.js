import React, { useState } from 'react';
import axios from 'axios';
import MovieCard from './MovieCard';

function LandingPage() {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [movies, setMovies] = useState([]);

  const handleSearch = async () => {
    setLoading(true);
    try {
      // Make POST request where the passed parameter as a list of queries
      const response = await axios.post('/api/find_movies', { queries: [query] });
      console.log(response);
      setMovies(response.data);
    } catch (error) {
      console.error('Error fetching movies:', error);
    }
    setLoading(false);
  };

  return (
    <div>
      <h1>Movie Search Engine</h1>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Enter movie title"
      />
      <button onClick={handleSearch} disabled={!query || loading}>
        Search
      </button>
      {loading && <div>Loading...</div>}
      <div style={{ display: 'flex', flexWrap: 'wrap' }}>
        {movies.map((movie) => (
          <MovieCard key={movie.WikipediaDocId} movie={movie} />
        ))}
      </div>
    </div>
  );
}

export default LandingPage;
