import React from 'react';

const MovieCard = ({ movie }) => {
  return (
    <div className="movie-card">
      <h2>{movie.MovieTitle}</h2>
      <p>Release Year: {movie.ReleaseDate}</p>
      <p>Runtime (in mins): {movie.Runtime}</p>
    </div>
  );
};

export default MovieCard;
