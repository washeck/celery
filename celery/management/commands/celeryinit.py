"""

Start the celery daemon from the Django management command.

"""
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    """Dummy command to init django after fork in celery."""
    help = 'Dummy command to initialize environment in the celery daemon'

    def handle(self, *args, **options):
        """Handle the management command."""
	print "running celeryinit"
