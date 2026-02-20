{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
	packages = with pkgs; [
		python3
		python3Packages.tqdm
		python3Packages.psycopg
		python3Packages.pymongo
	];
}
