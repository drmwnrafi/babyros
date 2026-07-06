import argparse
import sys
import time
from loguru import logger

def cmd_topics(args):
    """List all active topics in the Zenoh network."""
    try:
        import babyros
        
        print("Discovering active topics...")
        session = babyros.node.SessionManager.get_session()
        discovered_topics = set()
        print(f"Querying for active publishers (timeout: {args.timeout}s)...")
        
        try:
            # Query for all liveliness tokens using the liveliness API
            replies = session.liveliness().get("**/__liveliness__", timeout=args.timeout)
            
            for reply in replies:
                if reply.ok:
                    key_expr = str(reply.ok.key_expr)
                    logger.debug(f"Found liveliness token: {key_expr}")
                    
                    if "/__liveliness__" in key_expr:
                        topic = key_expr.split("/__liveliness__")[0]
                        discovered_topics.add(topic)
                else:
                    logger.debug(f"Reply error: {reply.err}")
                    
        except Exception as e:
            logger.error(f"Error during discovery: {e}")
            import traceback
            traceback.print_exc()
        
        if not discovered_topics:
            print("\nNo active topics found.")
        else:
            print(f"\nActive topics ({len(discovered_topics)}):")
            for topic in sorted(discovered_topics):
                print(f"  {topic}")
                
    except Exception as e:
        logger.error(f"Error discovering topics: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(prog='babyros', description='BabyROS command-line utilities')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Topics command
    parser_topics = subparsers.add_parser('topics', help='List all active topics')
    parser_topics.add_argument('-t', '--timeout', type=float, default=0.5, 
                              help='Discovery timeout in seconds (default: 0.5)')

    args = parser.parse_args()

    if args.command == 'topics':
        cmd_topics(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()