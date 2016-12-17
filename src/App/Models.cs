namespace Hugo.App
{
	public class Artist
	{
		public int ArtistId { get; set; }
		public string Name { get; set; }
	}

	public class Album
	{

		public int AlbumId { get; set; }
		public string Title { get; set; }
		public int ArtistId { get; set; }
	}

	public class Client
	{
		public int ClientId { get; set; }
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public string Email { get; set; }
	}

	public class Property
	{
		public int PropertyId { get; set; }
		public string Name { get; set; }
	}
}
