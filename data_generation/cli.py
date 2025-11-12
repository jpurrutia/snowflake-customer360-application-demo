"""
Command-line interface for customer data generation.

This module provides a CLI for generating synthetic customer data
using Click for argument parsing and command handling.
"""

import click
import sys
from pathlib import Path
from typing import Optional

from .customer_generator import generate_customers, validate_customer_data, save_to_csv
from .s3_uploader import upload_customers_to_s3, verify_s3_upload


@click.group()
def cli():
    """Customer 360 Analytics - Data Generation CLI"""
    pass


@cli.command(name="generate-customers")
@click.option(
    "--count",
    default=50000,
    type=int,
    help="Number of customers to generate (default: 50000)",
)
@click.option(
    "--output",
    default="customers.csv",
    type=str,
    help="Output file path (default: customers.csv)",
)
@click.option(
    "--seed",
    default=42,
    type=int,
    help="Random seed for reproducibility (default: 42)",
)
def generate_customers_command(count: int, output: str, seed: int):
    """
    Generate synthetic customer data for credit card portfolio.

    Examples:
        python -m data_generation generate-customers --count 1000 --output test.csv

        python -m data_generation generate-customers --count 50000 --seed 123
    """
    click.echo(f"Generating {count} customers with seed {seed}...")

    # Generate customers
    try:
        df = generate_customers(n=count, seed=seed)
        click.echo(f"✓ Generated {len(df)} customer records")
    except Exception as e:
        click.echo(f"✗ Error generating customers: {str(e)}", err=True)
        sys.exit(1)

    # Validate data
    click.echo("\nValidating customer data...")
    validation_result = validate_customer_data(df)

    # Print statistics
    stats = validation_result["statistics"]
    click.echo("\n📊 Statistics:")
    click.echo(f"  Total customers: {stats['total_customers']}")
    click.echo(f"  Unique IDs: {stats['unique_customer_ids']}")
    click.echo(f"  Credit limit range: ${stats['credit_limit_min']:,} - ${stats['credit_limit_max']:,}")
    click.echo(f"  Average credit limit: ${stats['credit_limit_avg']:,.2f}")

    click.echo("\n  Segment Distribution:")
    for segment, pct in stats["segment_distribution"].items():
        click.echo(f"    {segment}: {pct:.1%}")

    click.echo("\n  Card Type Distribution:")
    for card_type, count in stats["card_type_distribution"].items():
        pct = count / stats["total_customers"]
        click.echo(f"    {card_type}: {count} ({pct:.1%})")

    # Print warnings
    if validation_result["warnings"]:
        click.echo("\n⚠️  Warnings:")
        for warning in validation_result["warnings"]:
            click.echo(f"  - {warning}")

    # Print errors and exit if validation failed
    if not validation_result["is_valid"]:
        click.echo("\n✗ Validation FAILED:", err=True)
        for error in validation_result["errors"]:
            click.echo(f"  - {error}", err=True)
        sys.exit(1)

    click.echo("\n✓ Validation passed")

    # Save to CSV
    try:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_to_csv(df, output)
        click.echo(f"\n✓ Successfully saved to {output}")
    except Exception as e:
        click.echo(f"\n✗ Error saving file: {str(e)}", err=True)
        sys.exit(1)

    click.echo("\n🎉 Customer generation complete!")


@cli.command(name="upload-customers")
@click.option(
    "--file",
    required=True,
    type=str,
    help="Path to customer CSV file to upload",
)
@click.option(
    "--bucket",
    required=True,
    type=str,
    help="S3 bucket name (e.g., customer360-analytics-data-20250111)",
)
@click.option(
    "--profile",
    default=None,
    type=str,
    help="AWS profile name (optional, uses default if not specified)",
)
def upload_customers_command(file: str, bucket: str, profile: Optional[str]):
    """
    Upload customer CSV file to S3.

    Examples:
        python -m data_generation upload-customers \\
            --file data/customers.csv \\
            --bucket customer360-analytics-data-20250111

        python -m data_generation upload-customers \\
            --file data/customers.csv \\
            --bucket my-bucket \\
            --profile my-aws-profile
    """
    # Verify file exists first
    file_path = Path(file)
    if not file_path.exists():
        click.echo(f"✗ File not found: {file}", err=True)
        sys.exit(1)

    click.echo(f"Uploading {file} to S3 bucket: {bucket}")

    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    click.echo(f"  File size: {file_size_mb:.2f} MB")

    # Upload to S3
    click.echo("\nUploading to S3...")
    try:
        success = upload_customers_to_s3(
            csv_file=file,
            s3_bucket=bucket,
            aws_profile=profile
        )

        if success:
            # Verify upload
            s3_key = f"customers/{file_path.name}"
            click.echo("\nVerifying upload...")

            if verify_s3_upload(bucket, s3_key, profile):
                click.echo(f"\n✓ Upload successful!")
                click.echo(f"  S3 location: s3://{bucket}/{s3_key}")
                click.echo("\nNext steps:")
                click.echo("  1. Verify Snowflake can access the file:")
                click.echo("     LIST @CUSTOMER_ANALYTICS.BRONZE.customer_stage;")
                click.echo("  2. Load data into Snowflake Bronze layer")
            else:
                click.echo(f"\n⚠️  Upload completed but verification failed", err=True)
                sys.exit(1)
        else:
            click.echo(f"\n✗ Upload failed", err=True)
            sys.exit(1)

    except Exception as e:
        click.echo(f"\n✗ Upload error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
