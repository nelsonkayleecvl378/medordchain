const { Router } = require("express");
const accessController = require("../controllers/accessController");
const validateRequest = require("../middleware/validateRequest");
const { authenticate, requireRole } = require("../middleware/authMiddleware");
const { accessGrantSchema } = require("./validators");

const router = Router();

router.get("/", authenticate, accessController.listAccessGrants);
router.post(
  "/grant",
  authenticate,
  requireRole("patient"),
  validateRequest(accessGrantSchema),
  accessController.grantAccess
);
router.post(
  "/revoke",
  authenticate,
  requireRole("patient"),
  validateRequest(accessGrantSchema),
  accessController.revokeAccess
);

module.exports = router;
